package main

import (
	"archive/zip"
	"bytes"
	"crypto/sha1"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	baseURL     = "https://aisoip.adilet.gov.kz/extperson"
	workerCount = 8
)

var (
	httpClient     *http.Client
	httpClientErr  error
	httpClientOnce sync.Once
)

func getHTTPClient() (*http.Client, error) {
	httpClientOnce.Do(func() {
		pool, err := x509.SystemCertPool()
		if err != nil {
			httpClientErr = fmt.Errorf("не удалось загрузить системные сертификаты: %w", err)
			return
		}
		if pool == nil {
			pool = x509.NewCertPool()
		}

		extraCertFile := strings.TrimSpace(os.Getenv("EXTRA_CA_CERT_FILE"))
		if extraCertFile != "" {
			pemData, err := os.ReadFile(extraCertFile)
			if err != nil {
				httpClientErr = fmt.Errorf("не удалось прочитать EXTRA_CA_CERT_FILE=%q: %w", extraCertFile, err)
				return
			}
			if ok := pool.AppendCertsFromPEM(pemData); !ok {
				httpClientErr = fmt.Errorf("файл EXTRA_CA_CERT_FILE=%q не содержит PEM-сертификатов", extraCertFile)
				return
			}
			log.Printf("добавлен дополнительный CA сертификат из %s", extraCertFile)
		}

		transport := http.DefaultTransport.(*http.Transport).Clone()
		transport.TLSClientConfig = &tls.Config{
			MinVersion: tls.VersionTLS12,
			RootCAs:    pool,
		}

		httpClient = &http.Client{
			Timeout:   60 * time.Second,
			Transport: transport,
		}
	})

	return httpClient, httpClientErr
}

var exportColumns = []exportColumn{
	{Header: "Номер исполнительного производства", Path: "execProcNum"},
	{Header: "ИИН/БИН должника", Path: "debtorIinBin"},
	{Header: "ФИО должника", Path: "debtorFullName"},
	{Header: "Сумма долга в ИП в тг", Path: "recoveryAmount"},
	{Header: "Сумма долга в ИП в МРП", Path: "recoveryAmountMrp"},
	{Header: "Частичное погашение: ИП", Path: "collectedInfo.sumColOrder"},
	{Header: "Частичное погашение: Ручной ввод СИ", Path: "collectedInfo.sumManual"},
	{Header: "Частичное погашение: Взыскание с ЗП", Path: "collectedInfo.sumWageRec"},
	{Header: "Частичное погашение: Другие источники", Path: "collectedInfo.sumOther"},
}

var bankArrestColumns = []exportColumn{
	{Header: "Наименование БВУ", Path: "bank.name_ru"},
	{Header: "Уникальный идентификатор счёта", Path: "uniqueAccNumber"},
	{Header: "Последний статус ареста", Path: "arrestStatus.name_ru"},
	{Header: "Дата последнего изменения статуса ареста", Path: "arrestDate"},
	{Header: "Последний статус ИР", Path: "irStatus.name_ru"},
	{Header: "Дата последнего изменения статуса ИР", Path: "irDate"},
}

var notaryBanColumns = []exportColumn{
	{Header: "Статус постановления о наложении ареста", Path: "status.name_ru"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

var gcvpColumns = []exportColumn{
	{Header: "Дата", Path: "payDate"},
	{Header: "Сумма", Path: "amount"},
	{Header: "Работодатель", Path: "payerName"},
	{Header: "БИН работодателя", Path: "payerBin"},
}

var driverLicenseColumns = []exportColumn{
	{Header: "Наличие водительских прав", Path: "hasAutoDrDoc"},
	{Header: "Срок действия водительских прав", Path: "expireDate"},
}

var notificationColumns = []exportColumn{
	{Header: "Дата отправки", Path: "statusDate"},
	{Header: "Основание извещения должника", Path: "type.name_ru"},
	{Header: "Канал", Path: "channel.name_ru"},
	{Header: "Статус", Path: "status.name_ru"},
}

var autoInfoColumns = []exportColumn{
	{Header: "Ответ от ТС", Path: "arrestStatus.name_ru"},
	{Header: "Статус постановления ареста", Path: "status.name_ru"},
	{Header: "Количество арестованных ТС", Path: "objCount"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

var travelBanColumns = []exportColumn{
	{Header: "Дата извещения должника", Path: "notifDate"},
	{Header: "Статус постановления о наложении запрета", Path: "status.name_ru"},
	{Header: "Статус наложения запрета", Path: "arrestStatus.name_ru"},
	{Header: "Дата наложения запрета", Path: "banDate"},
	{Header: "Дата приостановления запрета", Path: "suspDate"},
	{Header: "Дата снятия запрета", Path: "unbanDate"},
}

var registrationBanColumns = []exportColumn{
	{Header: "Статус постановления о наложении ареста", Path: "status.name_ru"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

var propertyArrestColumns = []exportColumn{
	{Header: "Ответ от ГБД РН", Path: "arrestStatus.name_ru"},
	{Header: "Статус постановления ареста", Path: "status.name_ru"},
	{Header: "Количество арестованной недвижимости", Path: "objCount"},
	{Header: "Дата наложения ареста", Path: "banDate"},
	{Header: "Дата снятия ареста", Path: "unbanDate"},
}

type exportColumn struct {
	Header string
	Path   string
}

type sheetData struct {
	Name string
	Rows [][]string
}

type xlsxWorksheet struct {
	SheetData struct {
		Rows []xlsxRow `xml:"row"`
	} `xml:"sheetData"`
}

type xlsxRow struct {
	Cells []xlsxCell `xml:"c"`
}

type xlsxCell struct {
	Ref       string `xml:"r,attr"`
	Type      string `xml:"t,attr"`
	Value     string `xml:"v"`
	InlineStr struct {
		Text string `xml:"t"`
	} `xml:"is"`
}

type xlsxSharedStrings struct {
	Items []struct {
		Text string `xml:"t"`
		Runs []struct {
			Text string `xml:"t"`
		} `xml:"r"`
	} `xml:"si"`
}

type startRequest struct {
	Type       string          `json:"type"`
	SessionKey string          `json:"sessionKey"`
	FileName   string          `json:"fileName"`
	FileBase64 string          `json:"fileBase64"`
	SelectAll  bool            `json:"selectAll"`
	Options    map[string]bool `json:"options"`
}

type execExportRequest struct {
	Type        string   `json:"type"`
	SessionKey  string   `json:"sessionKey"`
	StartDate   string   `json:"startDate"`
	Statuses    []string `json:"statuses"`
	OutputFile  string   `json:"outputFile"`
	DownloadDir string   `json:"downloadDir"`
}

type downloadFile struct {
	FileName   string `json:"fileName"`
	FileBase64 string `json:"fileBase64"`
}

type wsMessage struct {
	Type               string         `json:"type"`
	Message            string         `json:"message,omitempty"`
	Current            int            `json:"current,omitempty"`
	Total              int            `json:"total,omitempty"`
	Number             string         `json:"number,omitempty"`
	Status             string         `json:"status,omitempty"`
	Error              string         `json:"error,omitempty"`
	FileName           string         `json:"fileName,omitempty"`
	FileBase64         string         `json:"fileBase64,omitempty"`
	Processed          int            `json:"processed,omitempty"`
	Failed             int            `json:"failed,omitempty"`
	ExtraFiles         []downloadFile `json:"extraFiles,omitempty"`
	LocalUnhandledFile string         `json:"localUnhandledFile,omitempty"`
}

type unhandledEntry struct {
	ExecProcNum     string         `json:"execProcNum"`
	DebtorFullName  string         `json:"debtorFullName"`
	DebtorIinBin    string         `json:"debtorIinBin"`
	UnhandledBlocks map[string]any `json:"unhandledBlocks"`
}

type job struct {
	Index  int
	Number string
}

type fetchResult struct {
	Index     int
	Number    string
	Parsed    map[string]any
	Err       error
	ResultRow []string
	Unhandled *unhandledEntry
}

func main() {
	addr := serverAddr()

	http.HandleFunc("/", serveIndex)
	http.HandleFunc("/ws", handleWebSocket)
	http.HandleFunc("/execproc-ws", handleExecProcWebSocket)

	log.Printf("HTTP server started on http://localhost%s", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		log.Fatal(err)
	}
}

func serverAddr() string {
	port := strings.TrimSpace(os.Getenv("PORT"))
	if port == "" {
		port = "8888"
	}
	if !strings.HasPrefix(port, ":") {
		port = ":" + port
	}
	return port
}

func serveIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, "index.html")
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req startRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос"})
		return
	}

	if strings.TrimSpace(req.SessionKey) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "ключ сессии обязателен"})
		return
	}
	if strings.TrimSpace(req.FileBase64) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "файл не был передан"})
		return
	}

	fileBytes, err := base64.StdEncoding.DecodeString(req.FileBase64)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось декодировать файл"})
		return
	}

	numbers, err := readNumbersFromXLSXBytes(fileBytes)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(numbers) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "в загруженном xlsx не найдено ни одного номера"})
		return
	}

	rows := make([][]string, 0, len(numbers)+1)
	header := make([]string, 0, len(exportColumns)+2)
	for _, column := range exportColumns {
		header = append(header, column.Header)
	}
	header = append(header, "Статус запроса", "Ошибка")
	rows = append(rows, header)

	includeBankArrest := req.SelectAll || req.Options["bankArrest"]
	bankArrestRows := make([][]string, 0)
	if includeBankArrest {
		bankHeader := make([]string, 0, len(bankArrestColumns)+3)
		bankHeader = append(bankHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range bankArrestColumns {
			bankHeader = append(bankHeader, column.Header)
		}
		bankArrestRows = append(bankArrestRows, bankHeader)
	}

	includeNotaryBan := req.SelectAll || req.Options["notaryBan"]
	notaryBanRows := make([][]string, 0)
	if includeNotaryBan {
		notaryHeader := make([]string, 0, len(notaryBanColumns)+3)
		notaryHeader = append(notaryHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range notaryBanColumns {
			notaryHeader = append(notaryHeader, column.Header)
		}
		notaryBanRows = append(notaryBanRows, notaryHeader)
	}

	includeGCVP := req.SelectAll || req.Options["gcvpPayments"]
	gcvpRows := make([][]string, 0)
	if includeGCVP {
		gcvpHeader := make([]string, 0, len(gcvpColumns)+4)
		gcvpHeader = append(gcvpHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника", "Категория")
		for _, column := range gcvpColumns {
			gcvpHeader = append(gcvpHeader, column.Header)
		}
		gcvpRows = append(gcvpRows, gcvpHeader)
	}

	includeDriverLicense := req.SelectAll || req.Options["driverLicense"]
	driverLicenseRows := make([][]string, 0)
	if includeDriverLicense {
		driverHeader := make([]string, 0, len(driverLicenseColumns)+3)
		driverHeader = append(driverHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range driverLicenseColumns {
			driverHeader = append(driverHeader, column.Header)
		}
		driverLicenseRows = append(driverLicenseRows, driverHeader)
	}

	includeNotifications := req.SelectAll || req.Options["notificationMethod"]
	notificationRows := make([][]string, 0)
	if includeNotifications {
		notificationHeader := make([]string, 0, len(notificationColumns)+3)
		notificationHeader = append(notificationHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range notificationColumns {
			notificationHeader = append(notificationHeader, column.Header)
		}
		notificationRows = append(notificationRows, notificationHeader)
	}

	includeAutoInfo := req.SelectAll || req.Options["transportArrest"]
	autoInfoRows := make([][]string, 0)
	if includeAutoInfo {
		autoInfoHeader := make([]string, 0, len(autoInfoColumns)+3)
		autoInfoHeader = append(autoInfoHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range autoInfoColumns {
			autoInfoHeader = append(autoInfoHeader, column.Header)
		}
		autoInfoRows = append(autoInfoRows, autoInfoHeader)
	}

	includeTravelBan := req.SelectAll || req.Options["travelBan"]
	travelBanRows := make([][]string, 0)
	if includeTravelBan {
		travelBanHeader := make([]string, 0, len(travelBanColumns)+3)
		travelBanHeader = append(travelBanHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range travelBanColumns {
			travelBanHeader = append(travelBanHeader, column.Header)
		}
		travelBanRows = append(travelBanRows, travelBanHeader)
	}

	includeRegistrationBan := req.SelectAll || req.Options["registrationBan"]
	registrationBanRows := make([][]string, 0)
	if includeRegistrationBan {
		registrationHeader := make([]string, 0, len(registrationBanColumns)+3)
		registrationHeader = append(registrationHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range registrationBanColumns {
			registrationHeader = append(registrationHeader, column.Header)
		}
		registrationBanRows = append(registrationBanRows, registrationHeader)
	}

	includePropertyArrest := req.SelectAll || req.Options["propertyArrest"]
	propertyArrestRows := make([][]string, 0)
	if includePropertyArrest {
		propertyHeader := make([]string, 0, len(propertyArrestColumns)+3)
		propertyHeader = append(propertyHeader, "Номер исполнительного производства", "ИИН/БИН должника", "ФИО должника")
		for _, column := range propertyArrestColumns {
			propertyHeader = append(propertyHeader, column.Header)
		}
		propertyArrestRows = append(propertyArrestRows, propertyHeader)
	}

	processed := 0
	failed := 0
	unhandledEntries := make([]unhandledEntry, 0)

	jobs := make(chan job)
	resultsCh := make(chan fetchResult, len(numbers))

	for i := 0; i < workerCount; i++ {
		go func() {
			for currentJob := range jobs {
				resultsCh <- processNumber(currentJob.Index, currentJob.Number, req.SessionKey, header)
			}
		}()
	}

	go func() {
		for idx, number := range numbers {
			jobs <- job{Index: idx, Number: number}
		}
		close(jobs)
	}()

	results := make([]fetchResult, len(numbers))
	for i := 0; i < len(numbers); i++ {
		result := <-resultsCh
		results[result.Index] = result
		_ = writeServerJSON(conn, wsMessage{
			Type:    "progress",
			Current: i + 1,
			Total:   len(numbers),
			Number:  result.Number,
			Status:  statusFromError(result.Err),
			Error:   errorText(result.Err),
		})
	}

	for _, result := range results {
		rows = append(rows, result.ResultRow)
		if result.Err != nil {
			failed++
			continue
		}

		processed++
		if includeBankArrest {
			bankArrestRows = appendBankArrestRows(bankArrestRows, result.Parsed, result.Number)
		}
		if includeNotaryBan {
			notaryBanRows = appendClEnisRows(notaryBanRows, result.Parsed, result.Number)
		}
		if includeGCVP {
			gcvpRows = appendGCVPRows(gcvpRows, result.Parsed, result.Number)
		}
		if includeDriverLicense {
			driverLicenseRows = appendDriverLicenseRows(driverLicenseRows, result.Parsed, result.Number)
		}
		if includeNotifications {
			notificationRows = appendNotificationRows(notificationRows, result.Parsed, result.Number)
		}
		if includeAutoInfo {
			autoInfoRows = appendAutoInfoRows(autoInfoRows, result.Parsed, result.Number)
		}
		if includeTravelBan {
			travelBanRows = appendTravelBanRows(travelBanRows, result.Parsed, result.Number)
		}
		if includeRegistrationBan {
			registrationBanRows = appendRegistrationBanRows(registrationBanRows, result.Parsed, result.Number)
		}
		if includePropertyArrest {
			propertyArrestRows = appendPropertyArrestRows(propertyArrestRows, result.Parsed, result.Number)
		}
		if result.Unhandled != nil {
			unhandledEntries = append(unhandledEntries, *result.Unhandled)
		}
	}

	sheets := []sheetData{{Name: "Результаты", Rows: rows}}
	if includeGCVP {
		sheets = append(sheets, sheetData{Name: "Выплаты/Пенсионные отчисления", Rows: gcvpRows})
	}
	if includeBankArrest {
		sheets = append(sheets, sheetData{Name: "Арест на банковские счета", Rows: bankArrestRows})
	}
	if includeTravelBan {
		sheets = append(sheets, sheetData{Name: "Временное ограничение на выезд", Rows: travelBanRows})
	}
	if includeAutoInfo {
		sheets = append(sheets, sheetData{Name: "Арест на транспорт", Rows: autoInfoRows})
	}
	if includePropertyArrest {
		sheets = append(sheets, sheetData{Name: "Арест на имущество", Rows: propertyArrestRows})
	}
	if includeNotaryBan {
		sheets = append(sheets, sheetData{Name: "Запрет на совершение нотариальных действий", Rows: notaryBanRows})
	}
	if includeRegistrationBan {
		sheets = append(sheets, sheetData{Name: "Запрет на регистрационные действия", Rows: registrationBanRows})
	}
	if includeDriverLicense {
		sheets = append(sheets, sheetData{Name: "Водительское удостоверение", Rows: driverLicenseRows})
	}
	if includeNotifications {
		sheets = append(sheets, sheetData{Name: "Способ уведомления должника (СМС/ЕТУ)", Rows: notificationRows})
	}

	xlsxBytes, err := buildXLSXBytes(sheets)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	resultMessage := wsMessage{
		Type:       "result",
		FileName:   "results.xlsx",
		FileBase64: base64.StdEncoding.EncodeToString(xlsxBytes),
		Processed:  processed,
		Failed:     failed,
	}
	if len(unhandledEntries) > 0 {
		payload, err := json.MarshalIndent(unhandledEntries, "", "  ")
		if err != nil {
			_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
			return
		}
		localFileName := buildUnhandledFileName()
		if err := os.WriteFile(localFileName, payload, 0644); err != nil {
			_ = writeServerJSON(conn, wsMessage{Type: "error", Message: fmt.Sprintf("не удалось сохранить локальный unhandled json: %v", err)})
			return
		}
		resultMessage.LocalUnhandledFile = localFileName
	}

	_ = writeServerJSON(conn, resultMessage)
}

func handleExecProcWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgradeToWebSocket(w, r)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	defer conn.Close()

	payload, err := readClientTextFrame(conn)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}

	var req execExportRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "не удалось разобрать запрос выгрузки"})
		return
	}

	if strings.TrimSpace(req.SessionKey) == "" {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "SESSION обязателен"})
		return
	}

	startDate := strings.TrimSpace(req.StartDate)
	if startDate == "" {
		startDate = "2017-01-01"
	}
	if _, err := time.Parse("2006-01-02", startDate); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "START_DATE должен быть в формате YYYY-MM-DD"})
		return
	}

	statuses := normalizeStatuses(req.Statuses)
	if len(statuses) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "выберите хотя бы один статус"})
		return
	}

	outputFile := strings.TrimSpace(req.OutputFile)
	if outputFile == "" {
		outputFile = "result.xlsx"
	}
	downloadDir := strings.TrimSpace(req.DownloadDir)
	if downloadDir == "" {
		downloadDir = "downloads"
	}
	errorLogFile := ""

	sendLog := func(message string) error {
		log.Print(message)
		if errorLogFile != "" && isProblemLog(message) {
			if err := appendLine(errorLogFile, time.Now().Format(time.RFC3339)+" "+message); err != nil {
				log.Printf("не удалось сохранить лог ошибки: %v", err)
			}
		}
		return writeServerJSON(conn, wsMessage{Type: "log", Message: message})
	}
	sendProgress := func(current, total int, status string) error {
		if current < 0 {
			current = 0
		}
		if current > total {
			current = total
		}
		return writeServerJSON(conn, wsMessage{
			Type:    "progress",
			Current: current,
			Total:   total,
			Status:  status,
		})
	}

	if err := os.MkdirAll(downloadDir, 0755); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: fmt.Sprintf("не удалось создать папку %s: %v", downloadDir, err)})
		return
	}
	errorLogFile = filepath.Join(downloadDir, "errors_"+time.Now().Format("20060102_150405")+".log")
	if err := sendLog("[INFO] Error log file: " + errorLogFile); err != nil {
		return
	}
	if err := sendProgress(1, 100, "Подготовка"); err != nil {
		return
	}

	files, stats, err := runExecProcExport(req.SessionKey, startDate, statuses, downloadDir, sendLog, sendProgress)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if len(files) == 0 {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: "по выбранным статусам данные не найдены"})
		return
	}

	if err := sendLog("[INFO] Merging Excel files..."); err != nil {
		return
	}
	if err := sendProgress(92, 100, "Объединение Excel"); err != nil {
		return
	}
	rows, removed, err := mergeExecProcFiles(files)
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Removing duplicates... removed: %d", removed)); err != nil {
		return
	}
	finalRows := len(rows) - 1
	if err := sendLog(fmt.Sprintf("[INFO] Full page totalElements: %d", stats.FullTotal)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Leaf range totalElements: %d", stats.LeafTotal)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Exported rows before merge dedupe: %d", stats.ExportedRows)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Final rows after dedupe: %d", finalRows)); err != nil {
		return
	}
	if err := sendLog(fmt.Sprintf("[INFO] Skipped ranges: %d", stats.SkippedRanges)); err != nil {
		return
	}
	if err := sendProgress(96, 100, "Сохранение результата"); err != nil {
		return
	}

	xlsxBytes, err := buildXLSXBytes([]sheetData{{Name: "Результаты", Rows: rows}})
	if err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: err.Error()})
		return
	}
	if err := os.WriteFile(outputFile, xlsxBytes, 0644); err != nil {
		_ = writeServerJSON(conn, wsMessage{Type: "error", Message: fmt.Sprintf("не удалось сохранить %s: %v", outputFile, err)})
		return
	}
	if err := sendLog("[INFO] Saved " + outputFile); err != nil {
		return
	}
	if err := sendProgress(100, 100, "Готово"); err != nil {
		return
	}

	_ = writeServerJSON(conn, wsMessage{
		Type:       "result",
		FileName:   filepath.Base(outputFile),
		FileBase64: base64.StdEncoding.EncodeToString(xlsxBytes),
		Processed:  finalRows,
		Failed:     removed,
	})
}

type exportStats struct {
	FullTotal     int
	LeafTotal     int
	ExportedRows  int
	SkippedRanges int
	mu            sync.Mutex
}

func (s *exportStats) addFullTotal(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.FullTotal += value
}

func (s *exportStats) addLeafTotal(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.LeafTotal += value
}

func (s *exportStats) addExportedRows(value int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ExportedRows += value
}

func (s *exportStats) addSkippedRange() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.SkippedRanges++
}

func runExecProcExport(session, startDate string, statuses []string, downloadDir string, sendLog func(string) error, sendProgress func(int, int, string) error) ([]string, *exportStats, error) {
	stats := &exportStats{}
	files := make([]string, 0)
	endDate := time.Now().Format("2006-01-02")

	for statusIndex, statusCode := range statuses {
		statusLabel := statusCode
		if statusLabel == "" {
			statusLabel = "all"
		}
		statusStart := 2 + (88*statusIndex)/len(statuses)
		statusPlanDone := statusStart + 22/len(statuses)
		statusEnd := 2 + (88*(statusIndex+1))/len(statuses)

		if err := sendProgress(statusStart, 100, "Планирование "+statusLabel); err != nil {
			return files, stats, err
		}

		fullRange := dateRange{
			From:  startDate,
			To:    endDate,
			Label: strings.ReplaceAll(startDate, "-", "_") + "_" + strings.ReplaceAll(endDate, "-", "_"),
		}
		fullSearchResult, err := searchDateRange(session, statusCode, fullRange, sendLog)
		if err != nil {
			return files, stats, err
		}
		stats.addFullTotal(fullSearchResult.TotalElements)
		if err := sendLog(fmt.Sprintf("[INFO] Status %s full page totalElements: %d", statusLabel, fullSearchResult.TotalElements)); err != nil {
			return files, stats, err
		}

		plannedRanges, err := planExportRanges(session, statusCode, statusLabel, fullRange, sendLog)
		if err != nil {
			return files, stats, err
		}
		if err := sendLog(fmt.Sprintf("[INFO] Planned ranges for status %s: %d", statusLabel, len(plannedRanges))); err != nil {
			return files, stats, err
		}
		if err := sendProgress(statusPlanDone, 100, "Выгрузка "+statusLabel); err != nil {
			return files, stats, err
		}

		downloadedFiles, err := downloadPlannedRanges(session, statusLabel, downloadDir, plannedRanges, sendLog, sendProgress, statusPlanDone, statusEnd, stats)
		if err != nil {
			return files, stats, err
		}
		files = append(files, downloadedFiles...)
		if err := sendProgress(statusEnd, 100, "Status "+statusLabel+" готов"); err != nil {
			return files, stats, err
		}
	}

	return files, stats, nil
}

func planExportRanges(session, statusCode, statusLabel string, currentRange dateRange, sendLog func(string) error) ([]plannedRange, error) {
	searchResult, err := searchDateRange(session, statusCode, currentRange, sendLog)
	if err != nil {
		return nil, wrapRangeError(statusLabel, currentRange, err)
	}
	if searchResult.TotalElements == 0 {
		return nil, nil
	}
	if searchResult.TotalElements <= 10000 {
		return []plannedRange{{Range: currentRange, Search: searchResult}}, nil
	}

	left, right, err := splitDateRange(currentRange)
	if err != nil {
		return nil, wrapRangeError(statusLabel, currentRange, err)
	}
	if err := sendLog(fmt.Sprintf("[INFO] Split range %s (%s - %s), totalElements=%d", currentRange.Label, currentRange.From, currentRange.To, searchResult.TotalElements)); err != nil {
		return nil, err
	}

	result := make([]plannedRange, 0)
	leftRanges, err := planExportRanges(session, statusCode, statusLabel, left, sendLog)
	if err != nil {
		return result, err
	}
	result = append(result, leftRanges...)
	rightRanges, err := planExportRanges(session, statusCode, statusLabel, right, sendLog)
	if err != nil {
		return result, err
	}
	result = append(result, rightRanges...)
	return result, nil
}

func downloadPlannedRanges(session, statusLabel, downloadDir string, ranges []plannedRange, sendLog func(string) error, sendProgress func(int, int, string) error, progressStart, progressEnd int, stats *exportStats) ([]string, error) {
	files := make([]string, 0, len(ranges))
	for idx, current := range ranges {
		progress := progressStart
		if len(ranges) > 0 {
			progress = progressStart + ((progressEnd - progressStart) * (idx + 1) / len(ranges))
		}
		if err := sendProgress(progress, 100, "Скачивание "+current.Range.Label); err != nil {
			return files, err
		}
		downloaded, err := exportPlannedRange(session, statusLabel, downloadDir, current, sendLog, stats)
		if err != nil {
			wrapped := wrapRangeError(statusLabel, current.Range, err)
			if logErr := sendLog("[ERROR] Skipped " + wrapped.Error()); logErr != nil {
				return files, logErr
			}
			stats.addSkippedRange()
			continue
		}
		files = append(files, downloaded...)
	}
	return files, nil
}

func exportPlannedRange(session, statusLabel, downloadDir string, current plannedRange, sendLog func(string) error, stats *exportStats) ([]string, error) {
	if strings.TrimSpace(current.Search.SearchID) == "" {
		return nil, fmt.Errorf("searchId пустой при totalElements=%d", current.Search.TotalElements)
	}

	fileName := filepath.Join(downloadDir, fmt.Sprintf("status_%s_%s.xlsx", statusLabel, current.Range.Label))
	if err := downloadExecProcExcel(session, current.Search.SearchID, fileName); err != nil {
		return nil, err
	}
	if err := sendLog("[INFO] Downloaded: " + fileName); err != nil {
		return nil, err
	}

	exportedRows, err := dataRowCountFromFile(fileName)
	if err != nil {
		return nil, err
	}
	if err := sendLog(fmt.Sprintf("[INFO] Exported rows: %d", exportedRows)); err != nil {
		return nil, err
	}
	stats.addLeafTotal(current.Search.TotalElements)
	stats.addExportedRows(exportedRows)
	if exportedRows > current.Search.TotalElements {
		if err := sendLog(fmt.Sprintf("[WARN] Range %s exported more rows than search totalElements: totalElements=%d exportedRows=%d", current.Range.Label, current.Search.TotalElements, exportedRows)); err != nil {
			return nil, err
		}
	}
	if current.Search.TotalElements > exportedRows && exportedRows >= 10000 {
		if err := sendLog(fmt.Sprintf("[WARN] Range %s may be truncated: totalElements=%d exportedRows=%d", current.Range.Label, current.Search.TotalElements, exportedRows)); err != nil {
			return nil, err
		}
	}

	return []string{fileName}, nil
}

func wrapRangeError(statusLabel string, currentRange dateRange, err error) error {
	return fmt.Errorf("status %s range %s (%s - %s): %w", statusLabel, currentRange.Label, currentRange.From, currentRange.To, err)
}

func searchDateRange(session, statusCode string, currentRange dateRange, sendLog func(string) error) (execProcSearchResult, error) {
	searchResult, err := searchExecProc(session, currentRange.From, currentRange.To, statusCode, sendLog)
	if err != nil {
		return execProcSearchResult{}, err
	}
	if err := sendLog(fmt.Sprintf("[INFO] Found totalElements: %d", searchResult.TotalElements)); err != nil {
		return execProcSearchResult{}, err
	}
	if err := sendLog("[INFO] searchId: " + searchResult.SearchID); err != nil {
		return execProcSearchResult{}, err
	}
	return searchResult, nil
}

type dateRange struct {
	From  string
	To    string
	Label string
}

type plannedRange struct {
	Range  dateRange
	Search execProcSearchResult
}

func splitDateRange(currentRange dateRange) (dateRange, dateRange, error) {
	start, err := time.Parse("2006-01-02", currentRange.From)
	if err != nil {
		return dateRange{}, dateRange{}, err
	}
	end, err := time.Parse("2006-01-02", currentRange.To)
	if err != nil {
		return dateRange{}, dateRange{}, err
	}
	if !start.Before(end) {
		return dateRange{}, dateRange{}, fmt.Errorf("диапазон %s нельзя разделить дальше", currentRange.Label)
	}

	days := int(end.Sub(start).Hours()/24) + 1
	leftDays := days / 2
	if leftDays < 1 {
		leftDays = 1
	}
	leftEnd := start.AddDate(0, 0, leftDays-1)
	rightStart := leftEnd.AddDate(0, 0, 1)
	return newDateRange(start, leftEnd), newDateRange(rightStart, end), nil
}

func newDateRange(start, end time.Time) dateRange {
	from := start.Format("2006-01-02")
	to := end.Format("2006-01-02")
	label := strings.ReplaceAll(from, "-", "_")
	if from != to {
		label += "_" + strings.ReplaceAll(to, "-", "_")
	}
	return dateRange{
		From:  from,
		To:    to,
		Label: label,
	}
}

type execProcSearchResult struct {
	TotalElements int
	SearchID      string
}

func searchExecProc(session, fromDate, toDate, statusCode string, sendLog func(string) error) (execProcSearchResult, error) {
	searchType := strings.TrimSpace(statusCode) != ""
	payload := map[string]any{
		"fromDate":   fromDate,
		"toDate":     toDate,
		"searchType": searchType,
	}
	if strings.TrimSpace(statusCode) != "" {
		payload["statusCode"] = statusCode
	}
	body, err := json.Marshal(payload)
	if err != nil {
		return execProcSearchResult{}, err
	}
	if sendLog != nil {
		if err := sendLog("[DEBUG] Search curl: " + buildSearchCurl(session, string(body))); err != nil {
			return execProcSearchResult{}, err
		}
	}

	req, err := http.NewRequest(http.MethodPost, baseURL+"/api/rest/execproc/search?page=0&size=5", bytes.NewReader(body))
	if err != nil {
		return execProcSearchResult{}, err
	}
	addExecProcHeaders(req, session)
	req.Header.Set("Content-Type", "application/json")

	client, err := getHTTPClient()
	if err != nil {
		return execProcSearchResult{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return execProcSearchResult{}, fmt.Errorf("ошибка поиска status=%s from=%s: %w", statusCode, fromDate, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return execProcSearchResult{}, fmt.Errorf("не удалось прочитать ответ поиска: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return execProcSearchResult{}, fmt.Errorf("поиск вернул статус %d: %s", resp.StatusCode, strings.TrimSpace(string(respBody)))
	}

	var parsed struct {
		Pagination struct {
			TotalElements int    `json:"totalElements"`
			SearchID      string `json:"searchId"`
		} `json:"pagination"`
	}
	if err := json.Unmarshal(respBody, &parsed); err != nil {
		return execProcSearchResult{}, fmt.Errorf("не удалось разобрать ответ поиска: %w", err)
	}

	return execProcSearchResult{
		TotalElements: parsed.Pagination.TotalElements,
		SearchID:      parsed.Pagination.SearchID,
	}, nil
}

func buildSearchCurl(session, body string) string {
	return "curl 'https://aisoip.adilet.gov.kz/extperson/api/rest/execproc/search?page=0&size=5' " +
		"-H 'Accept: application/json, text/plain, */*' " +
		"-H 'Accept-Language: ru' " +
		"-H 'Content-Type: application/json' " +
		"-b 'SESSION=" + maskSecret(session) + "' " +
		"-H 'Origin: https://aisoip.adilet.gov.kz' " +
		"-H 'Referer: https://aisoip.adilet.gov.kz/cabinet/exec-productions' " +
		"--data-raw '" + strings.ReplaceAll(body, "'", "'\\''") + "'"
}

func maskSecret(value string) string {
	value = strings.TrimSpace(value)
	if len(value) <= 12 {
		return "***"
	}
	return value[:6] + "..." + value[len(value)-6:]
}

func downloadExecProcExcel(session, searchID, fileName string) error {
	u, err := url.Parse(baseURL + "/api/rest/export/excel")
	if err != nil {
		return err
	}
	query := u.Query()
	query.Set("searchtype", "false")
	query.Set("page", "0")
	query.Set("size", "50000")
	query.Set("searchid", searchID)
	query.Set("lang", "ru")
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return err
	}
	addExecProcHeaders(req, session)

	client, err := getHTTPClient()
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка скачивания Excel searchId=%s: %w", searchID, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("не удалось прочитать Excel: %w", err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("Excel endpoint вернул статус %d: %s", resp.StatusCode, strings.TrimSpace(string(data)))
	}
	if len(data) < 4 || string(data[:2]) != "PK" {
		return fmt.Errorf("Excel не скачался: ответ не похож на .xlsx, размер %d байт", len(data))
	}

	if err := os.WriteFile(fileName, data, 0644); err != nil {
		return fmt.Errorf("не удалось сохранить %s: %w", fileName, err)
	}
	return nil
}

func addExecProcHeaders(req *http.Request, session string) {
	req.AddCookie(&http.Cookie{Name: "SESSION", Value: session})
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("Origin", "https://aisoip.adilet.gov.kz")
	req.Header.Set("Referer", "https://aisoip.adilet.gov.kz/cabinet/exec-productions")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")
}

func normalizeStatuses(statuses []string) []string {
	allowed := map[string]struct{}{"1": {}, "2": {}, "3": {}, "50": {}, "51": {}, "52": {}}
	if len(statuses) == 0 {
		return []string{""}
	}
	seen := make(map[string]struct{})
	result := make([]string, 0, len(statuses))
	for _, status := range statuses {
		status = strings.TrimSpace(status)
		if status == "" {
			return []string{""}
		}
		if _, ok := allowed[status]; !ok {
			continue
		}
		if _, ok := seen[status]; ok {
			continue
		}
		seen[status] = struct{}{}
		result = append(result, status)
	}
	return result
}

func maxExcitationDateFromFile(fileName string) (string, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return "", err
	}
	rows, err := readXLSXRowsBytes(data)
	if err != nil {
		return "", err
	}
	if len(rows) == 0 {
		return "", nil
	}
	dateIndex := findHeaderIndex(rows[0], "Дата возбуждения")
	if dateIndex < 0 {
		return "", errors.New("в Excel нет колонки «Дата возбуждения»")
	}
	maxDate := ""
	for i := 1; i < len(rows); i++ {
		if dateIndex >= len(rows[i]) {
			continue
		}
		value := normalizeExcelDate(rows[i][dateIndex])
		if value > maxDate {
			maxDate = value
		}
	}
	return maxDate, nil
}

func dataRowCountFromFile(fileName string) (int, error) {
	data, err := os.ReadFile(fileName)
	if err != nil {
		return 0, err
	}
	rows, err := readXLSXRowsBytes(data)
	if err != nil {
		return 0, err
	}
	count := 0
	for _, row := range rows[1:] {
		if rowHasAnyValue(row) {
			count++
		}
	}
	return count, nil
}

func rowHasAnyValue(row []string) bool {
	for _, value := range row {
		if strings.TrimSpace(value) != "" {
			return true
		}
	}
	return false
}

func mergeExecProcFiles(files []string) ([][]string, int, error) {
	files = append([]string(nil), files...)
	sort.Strings(files)

	var header []string
	uniqueRows := make([][]string, 0)
	seen := make(map[string]string)
	removed := 0

	for _, fileName := range files {
		data, err := os.ReadFile(fileName)
		if err != nil {
			return nil, removed, err
		}
		rows, err := readXLSXRowsBytes(data)
		if err != nil {
			return nil, removed, fmt.Errorf("%s: %w", fileName, err)
		}
		if len(rows) == 0 {
			continue
		}
		if header == nil {
			header = rows[0]
		}
		keyIndexes := duplicateKeyIndexes(rows[0])
		for _, row := range rows[1:] {
			key := duplicateKey(row, keyIndexes)
			if strings.TrimSpace(key) == "" {
				key = duplicateFallbackKey(row)
			}
			if firstFile, ok := seen[key]; ok {
				if firstFile != fileName {
					removed++
					continue
				}
			} else {
				seen[key] = fileName
			}
			uniqueRows = append(uniqueRows, row)
		}
	}

	if header == nil {
		return nil, removed, errors.New("нет строк для объединения")
	}
	return append([][]string{header}, uniqueRows...), removed, nil
}

func duplicateKeyIndexes(header []string) []int {
	if idx := findHeaderIndex(header, "execProcId"); idx >= 0 {
		return []int{idx}
	}
	return nil
}

func duplicateFallbackKey(row []string) string {
	if len(row) <= 1 {
		return strings.Join(row, "\x1f")
	}
	return strings.Join(row[1:], "\x1f")
}

func duplicateKey(row []string, indexes []int) string {
	parts := make([]string, 0, len(indexes))
	for _, idx := range indexes {
		if idx >= len(row) {
			parts = append(parts, "")
			continue
		}
		parts = append(parts, strings.TrimSpace(row[idx]))
	}
	return strings.Join(parts, "\x1f")
}

func findHeaderIndex(header []string, name string) int {
	target := strings.ToLower(strings.TrimSpace(name))
	for idx, value := range header {
		if strings.ToLower(strings.TrimSpace(value)) == target {
			return idx
		}
	}
	return -1
}

func findFirstHeaderIndex(header []string, names ...string) int {
	for _, name := range names {
		if idx := findHeaderIndex(header, name); idx >= 0 {
			return idx
		}
	}
	return -1
}

func readNumbersFromXLSXBytes(data []byte) ([]string, error) {
	rows, err := readXLSXRowsBytes(data)
	if err == nil {
		numbers := make([]string, 0, len(rows))
		for _, row := range rows {
			if len(row) == 0 {
				continue
			}
			value := strings.TrimSpace(row[0])
			if value == "" {
				continue
			}
			lowerValue := strings.ToLower(value)
			if lowerValue == "number" || strings.Contains(lowerValue, "номер") {
				continue
			}
			numbers = append(numbers, value)
		}
		return numbers, nil
	}

	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("не удалось открыть xlsx: %w", err)
	}

	sharedStrings, _ := readSharedStrings(reader)
	sheetXML, err := readZipFile(reader, "xl/worksheets/sheet1.xml")
	if err != nil {
		return nil, err
	}

	var sheet xlsxWorksheet
	if err := xml.Unmarshal(sheetXML, &sheet); err != nil {
		return nil, fmt.Errorf("не удалось прочитать sheet1.xml: %w", err)
	}

	numbers := make([]string, 0, len(sheet.SheetData.Rows))
	for _, row := range sheet.SheetData.Rows {
		if len(row.Cells) == 0 {
			continue
		}
		value := strings.TrimSpace(resolveCellValue(row.Cells[0], sharedStrings))
		if value == "" {
			continue
		}
		lowerValue := strings.ToLower(value)
		if lowerValue == "number" || strings.Contains(lowerValue, "номер") {
			continue
		}
		numbers = append(numbers, value)
	}

	return numbers, nil
}

func readXLSXRowsBytes(data []byte) ([][]string, error) {
	reader, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("не удалось открыть xlsx: %w", err)
	}

	sharedStrings, _ := readSharedStrings(reader)
	sheetXML, err := readZipFile(reader, "xl/worksheets/sheet1.xml")
	if err != nil {
		return nil, err
	}

	var sheet xlsxWorksheet
	if err := xml.Unmarshal(sheetXML, &sheet); err != nil {
		return nil, fmt.Errorf("не удалось прочитать sheet1.xml: %w", err)
	}

	rows := make([][]string, 0, len(sheet.SheetData.Rows))
	for _, sourceRow := range sheet.SheetData.Rows {
		row := make([]string, 0, len(sourceRow.Cells))
		for fallbackIdx, cell := range sourceRow.Cells {
			colIdx := fallbackIdx
			if parsedIdx := cellColumnIndex(cell.Ref); parsedIdx >= 0 {
				colIdx = parsedIdx
			}
			for len(row) <= colIdx {
				row = append(row, "")
			}
			row[colIdx] = strings.TrimSpace(resolveCellValue(cell, sharedStrings))
		}
		rows = append(rows, row)
	}

	return rows, nil
}

func cellColumnIndex(ref string) int {
	ref = strings.TrimSpace(ref)
	if ref == "" {
		return -1
	}
	result := 0
	seenLetter := false
	for _, r := range ref {
		switch {
		case r >= 'A' && r <= 'Z':
			result = result*26 + int(r-'A'+1)
			seenLetter = true
		case r >= 'a' && r <= 'z':
			result = result*26 + int(r-'a'+1)
			seenLetter = true
		default:
			if seenLetter {
				return result - 1
			}
		}
	}
	if !seenLetter {
		return -1
	}
	return result - 1
}

func readSharedStrings(reader *zip.Reader) ([]string, error) {
	data, err := readZipFile(reader, "xl/sharedStrings.xml")
	if err != nil {
		return nil, err
	}

	var shared xlsxSharedStrings
	if err := xml.Unmarshal(data, &shared); err != nil {
		return nil, fmt.Errorf("не удалось прочитать sharedStrings.xml: %w", err)
	}

	result := make([]string, 0, len(shared.Items))
	for _, item := range shared.Items {
		if item.Text != "" {
			result = append(result, item.Text)
			continue
		}
		var builder strings.Builder
		for _, run := range item.Runs {
			builder.WriteString(run.Text)
		}
		result = append(result, builder.String())
	}

	return result, nil
}

func readZipFile(reader *zip.Reader, name string) ([]byte, error) {
	for _, file := range reader.File {
		if file.Name != name {
			continue
		}
		rc, err := file.Open()
		if err != nil {
			return nil, fmt.Errorf("не удалось открыть %s: %w", name, err)
		}
		defer rc.Close()

		data, err := io.ReadAll(rc)
		if err != nil {
			return nil, fmt.Errorf("не удалось прочитать %s: %w", name, err)
		}
		return data, nil
	}

	return nil, fmt.Errorf("в xlsx не найден файл %s", name)
}

func resolveCellValue(cell xlsxCell, sharedStrings []string) string {
	switch cell.Type {
	case "inlineStr":
		return cell.InlineStr.Text
	case "s":
		index, err := strconv.Atoi(strings.TrimSpace(cell.Value))
		if err != nil || index < 0 || index >= len(sharedStrings) {
			return ""
		}
		return sharedStrings[index]
	default:
		return cell.Value
	}
}

func fetchArrestInfo(baseURL, session, execProcNum string) (map[string]any, error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return nil, fmt.Errorf("некорректный baseURL: %w", err)
	}

	u.Path = strings.TrimRight(u.Path, "/") + "/api/rest/claimant/arrestInfo"
	query := u.Query()
	query.Set("execProcNum", execProcNum)
	u.RawQuery = query.Encode()

	req, err := http.NewRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("не удалось создать запрос: %w", err)
	}

	req.AddCookie(&http.Cookie{Name: "SESSION", Value: session})
	req.Header.Set("Accept", "application/json, text/plain, */*")
	req.Header.Set("Accept-Language", "ru")
	req.Header.Set("Referer", strings.TrimRight(baseURL, "/")+"/cabinet/claimant-arrests")
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/135.0.0.0 Safari/537.36")

	client, err := getHTTPClient()
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка запроса: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("не удалось прочитать ответ: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("сервер вернул статус %d: %s", resp.StatusCode, strings.TrimSpace(string(body)))
	}

	var parsed map[string]any
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.UseNumber()
	if err := decoder.Decode(&parsed); err != nil {
		return nil, fmt.Errorf("не удалось разобрать JSON: %w", err)
	}

	return parsed, nil
}

func processNumber(index int, number, sessionKey string, header []string) fetchResult {
	resultRow := make([]string, 0, len(header))
	parsed, err := fetchArrestInfo(baseURL, sessionKey, number)
	if err != nil {
		for _, column := range exportColumns {
			if column.Path == "execProcNum" {
				resultRow = append(resultRow, number)
				continue
			}
			resultRow = append(resultRow, "")
		}
		resultRow = append(resultRow, "Ошибка", err.Error())
		return fetchResult{
			Index:     index,
			Number:    number,
			Err:       err,
			ResultRow: resultRow,
		}
	}

	for _, column := range exportColumns {
		value := extractPathValue(parsed, column.Path)
		if column.Path == "execProcNum" && value == "" {
			value = number
		}
		resultRow = append(resultRow, value)
	}
	resultRow = append(resultRow, "OK", "")

	var unhandledEntryValue *unhandledEntry
	if unhandled := collectUnhandledData(parsed); len(unhandled) > 0 {
		unhandledEntryValue = &unhandledEntry{
			ExecProcNum:     firstNonEmpty(extractPathValue(parsed, "execProcNum"), number),
			DebtorFullName:  extractPathValue(parsed, "debtorFullName"),
			DebtorIinBin:    extractPathValue(parsed, "debtorIinBin"),
			UnhandledBlocks: unhandled,
		}
	}

	return fetchResult{
		Index:     index,
		Number:    number,
		Parsed:    parsed,
		Err:       nil,
		ResultRow: resultRow,
		Unhandled: unhandledEntryValue,
	}
}

func statusFromError(err error) string {
	if err != nil {
		return "Ошибка"
	}
	return "OK"
}

func errorText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func extractPathValue(data map[string]any, path string) string {
	var current any = data
	for _, part := range strings.Split(path, ".") {
		mapped, ok := current.(map[string]any)
		if !ok {
			return ""
		}
		current, ok = mapped[part]
		if !ok {
			return ""
		}
	}
	return stringifyValue(current)
}

func collectUnhandledData(parsed map[string]any) map[string]any {
	handledTopLevel := map[string]struct{}{
		"execProcNum":       {},
		"debtorFullName":    {},
		"debtorIinBin":      {},
		"recoveryAmount":    {},
		"recoveryAmountMrp": {},
		"collectedInfo":     {},
		"gcvpDetail":        {},
		"smsNotifLists":     {},
		"clEnisInfo":        {},
		"clFlUlRegInfo":     {},
		"tradeInfo":         {},
		"autoInfo":          {},
		"rnInfo":            {},
		"autoDrInfoDto":     {},
		"travelBanInfo":     {},
		"bankBanInfo":       {},
	}

	unhandled := make(map[string]any)
	for key, value := range parsed {
		if _, ok := handledTopLevel[key]; ok {
			continue
		}
		if hasMeaningfulValue(value) {
			unhandled[key] = value
		}
	}

	if gcvpRaw, ok := parsed["gcvpDetail"].(map[string]any); ok {
		extra := make(map[string]any)
		handledGCVPKeys := map[string]struct{}{
			"clGCVPInfo":               {},
			"clGCVPPaymentPensionDtos": {},
		}
		for key, value := range gcvpRaw {
			if _, ok := handledGCVPKeys[key]; !ok && hasMeaningfulValue(value) {
				extra[key] = value
			}
		}
		if hasMeaningfulValue(gcvpRaw["clGCVPInfo"]) {
			extra["clGCVPInfo"] = gcvpRaw["clGCVPInfo"]
		}
		if len(extra) > 0 {
			unhandled["gcvpDetail"] = extra
		}
	}

	if hasMeaningfulValue(parsed["tradeInfo"]) {
		unhandled["tradeInfo"] = parsed["tradeInfo"]
	}
	return unhandled
}

func hasMeaningfulValue(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case string:
		return strings.TrimSpace(typed) != ""
	case []any:
		return len(typed) > 0
	case map[string]any:
		if len(typed) == 0 {
			return false
		}
		for _, item := range typed {
			if hasMeaningfulValue(item) {
				return true
			}
		}
		return false
	default:
		return true
	}
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return value
		}
	}
	return ""
}

func buildUnhandledFileName() string {
	return "unhandled_data_" + time.Now().Format("20060102_150405") + ".json"
}

func isProblemLog(message string) bool {
	return strings.HasPrefix(message, "[ERROR]") || strings.HasPrefix(message, "[WARN]")
}

func appendLine(fileName, line string) error {
	file, err := os.OpenFile(fileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	_, err = file.WriteString(line + "\n")
	return err
}

func appendBankArrestRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["bankBanInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range bankArrestColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "arrestDate" || column.Path == "irDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendClEnisRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["clEnisInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range notaryBanColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendGCVPRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	root, ok := parsed["gcvpDetail"].(map[string]any)
	if !ok {
		return rows
	}

	items, ok := root["clGCVPPaymentPensionDtos"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{
			execProcNum,
			debtorIinBin,
			debtorFullName,
			detectGCVPCategory(mapped),
		}
		for _, column := range gcvpColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "payDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendDriverLicenseRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	switch item := parsed["autoDrInfoDto"].(type) {
	case map[string]any:
		if len(item) == 0 {
			return rows
		}
		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range driverLicenseColumns {
			value := extractPathValue(item, column.Path)
			if column.Path == "expireDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	case []any:
		for _, raw := range item {
			mapped, ok := raw.(map[string]any)
			if !ok || len(mapped) == 0 {
				continue
			}
			row := []string{execProcNum, debtorIinBin, debtorFullName}
			for _, column := range driverLicenseColumns {
				value := extractPathValue(mapped, column.Path)
				if column.Path == "expireDate" {
					value = formatDisplayDate(value)
				}
				row = append(row, value)
			}
			rows = append(rows, row)
		}
	}

	return rows
}

func appendNotificationRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["smsNotifLists"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range notificationColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "statusDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendAutoInfoRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["autoInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range autoInfoColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendTravelBanRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["travelBanInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range travelBanColumns {
			value := extractPathValue(mapped, column.Path)
			switch column.Path {
			case "notifDate", "banDate", "suspDate", "unbanDate":
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendRegistrationBanRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["clFlUlRegInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range registrationBanColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func appendPropertyArrestRows(rows [][]string, parsed map[string]any, fallbackExecProcNum string) [][]string {
	items, ok := parsed["rnInfo"].([]any)
	if !ok || len(items) == 0 {
		return rows
	}

	execProcNum := extractPathValue(parsed, "execProcNum")
	if execProcNum == "" {
		execProcNum = fallbackExecProcNum
	}
	debtorIinBin := extractPathValue(parsed, "debtorIinBin")
	debtorFullName := extractPathValue(parsed, "debtorFullName")

	for _, item := range items {
		mapped, ok := item.(map[string]any)
		if !ok {
			continue
		}

		row := []string{execProcNum, debtorIinBin, debtorFullName}
		for _, column := range propertyArrestColumns {
			value := extractPathValue(mapped, column.Path)
			if column.Path == "banDate" || column.Path == "unbanDate" {
				value = formatDisplayDate(value)
			}
			row = append(row, value)
		}
		rows = append(rows, row)
	}

	return rows
}

func detectGCVPCategory(item map[string]any) string {
	candidates := []string{
		extractPathValue(item, "type.name_ru"),
		extractPathValue(item, "type"),
		extractPathValue(item, "category.name_ru"),
		extractPathValue(item, "category"),
	}

	for _, candidate := range candidates {
		value := strings.TrimSpace(candidate)
		if value == "" {
			continue
		}

		lower := strings.ToLower(value)
		switch {
		case strings.Contains(lower, "pension"), strings.Contains(lower, "пенс"):
			return "Пенсионка"
		case strings.Contains(lower, "payment"), strings.Contains(lower, "плат"):
			return "Платеж"
		default:
			return value
		}
	}

	return "Платеж"
}

func formatDisplayDate(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
	}
	for _, format := range formats {
		parsed, err := time.Parse(format, value)
		if err == nil {
			return parsed.Format("02.01.2006")
		}
	}

	if len(value) >= len("2006-01-02") {
		prefix := value[:10]
		parsed, err := time.Parse("2006-01-02", prefix)
		if err == nil {
			return parsed.Format("02.01.2006")
		}
	}

	return value
}

func normalizeExcelDate(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}

	formats := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02",
		"02.01.2006",
		"02/01/2006",
		"01/02/2006",
	}
	for _, format := range formats {
		parsed, err := time.Parse(format, value)
		if err == nil {
			return parsed.Format("2006-01-02")
		}
	}

	if len(value) >= len("2006-01-02") {
		prefix := value[:10]
		parsed, err := time.Parse("2006-01-02", prefix)
		if err == nil {
			return parsed.Format("2006-01-02")
		}
	}

	if serial, err := strconv.ParseFloat(strings.ReplaceAll(value, ",", "."), 64); err == nil && serial > 1 {
		base := time.Date(1899, 12, 30, 0, 0, 0, 0, time.UTC)
		return base.Add(time.Duration(serial*24) * time.Hour).Format("2006-01-02")
	}

	return ""
}

func stringifyValue(v any) string {
	switch typed := v.(type) {
	case nil:
		return ""
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		if typed {
			return "Да"
		}
		return "Нет"
	default:
		return fmt.Sprintf("%v", typed)
	}
}

func buildXLSXBytes(sheets []sheetData) ([]byte, error) {
	var buffer bytes.Buffer
	zipWriter := zip.NewWriter(&buffer)

	files := map[string]string{
		"[Content_Types].xml":        contentTypesXML(len(sheets)),
		"_rels/.rels":                rootRelsXML(),
		"docProps/app.xml":           appXML(sheets),
		"docProps/core.xml":          coreXML(),
		"xl/workbook.xml":            workbookXML(sheets),
		"xl/_rels/workbook.xml.rels": workbookRelsXML(len(sheets)),
		"xl/styles.xml":              stylesXML(),
	}

	for idx, sheet := range sheets {
		files[fmt.Sprintf("xl/worksheets/sheet%d.xml", idx+1)] = worksheetXML(sheet.Rows)
	}

	for _, name := range orderedFileNames(files) {
		writer, err := zipWriter.Create(name)
		if err != nil {
			return nil, fmt.Errorf("не удалось добавить %s в архив: %w", name, err)
		}
		if _, err := writer.Write([]byte(files[name])); err != nil {
			return nil, fmt.Errorf("не удалось записать %s: %w", name, err)
		}
	}

	if err := zipWriter.Close(); err != nil {
		return nil, fmt.Errorf("не удалось закрыть Excel архив: %w", err)
	}

	return buffer.Bytes(), nil
}

func orderedFileNames(files map[string]string) []string {
	names := make([]string, 0, len(files))
	for name := range files {
		names = append(names, name)
	}
	for i := 0; i < len(names); i++ {
		for j := i + 1; j < len(names); j++ {
			if names[j] < names[i] {
				names[i], names[j] = names[j], names[i]
			}
		}
	}
	return names
}

func contentTypesXML(sheetCount int) string {
	var overrides strings.Builder
	overrides.WriteString(`<Override PartName="/xl/workbook.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>`)
	overrides.WriteString(`<Override PartName="/xl/styles.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>`)
	overrides.WriteString(`<Override PartName="/docProps/core.xml" ContentType="application/vnd.openxmlformats-package.core-properties+xml"/>`)
	overrides.WriteString(`<Override PartName="/docProps/app.xml" ContentType="application/vnd.openxmlformats-officedocument.extended-properties+xml"/>`)
	for i := 1; i <= sheetCount; i++ {
		overrides.WriteString(fmt.Sprintf(`<Override PartName="/xl/worksheets/sheet%d.xml" ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>`, i))
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">` +
		`<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>` +
		`<Default Extension="xml" ContentType="application/xml"/>` +
		overrides.String() +
		`</Types>`
}

func rootRelsXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">` +
		`<Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" Target="xl/workbook.xml"/>` +
		`<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties" Target="docProps/core.xml"/>` +
		`<Relationship Id="rId3" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties" Target="docProps/app.xml"/>` +
		`</Relationships>`
}

func appXML(sheets []sheetData) string {
	var titles strings.Builder
	for _, sheet := range sheets {
		titles.WriteString(`<vt:lpstr>` + xmlEscape(sheet.Name) + `</vt:lpstr>`)
	}
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<Properties xmlns="http://schemas.openxmlformats.org/officeDocument/2006/extended-properties" xmlns:vt="http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes">` +
		`<Application>Go</Application>` +
		`<TitlesOfParts><vt:vector size="` + strconv.Itoa(len(sheets)) + `" baseType="lpstr">` + titles.String() + `</vt:vector></TitlesOfParts>` +
		`</Properties>`
}

func coreXML() string {
	now := time.Now().UTC().Format(time.RFC3339)
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<cp:coreProperties xmlns:cp="http://schemas.openxmlformats.org/package/2006/metadata/core-properties" xmlns:dc="http://purl.org/dc/elements/1.1/" xmlns:dcterms="http://purl.org/dc/terms/" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">` +
		`<dc:creator>Codex</dc:creator>` +
		`<cp:lastModifiedBy>Codex</cp:lastModifiedBy>` +
		`<dcterms:created xsi:type="dcterms:W3CDTF">` + now + `</dcterms:created>` +
		`<dcterms:modified xsi:type="dcterms:W3CDTF">` + now + `</dcterms:modified>` +
		`</cp:coreProperties>`
}

func workbookXML(sheets []sheetData) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	builder.WriteString(`<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships"><sheets>`)
	for idx, sheet := range sheets {
		builder.WriteString(fmt.Sprintf(`<sheet name="%s" sheetId="%d" r:id="rId%d"/>`, xmlEscape(safeSheetName(sheet.Name, idx+1)), idx+1, idx+1))
	}
	builder.WriteString(`</sheets></workbook>`)
	return builder.String()
}

func workbookRelsXML(sheetCount int) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	builder.WriteString(`<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">`)
	for i := 1; i <= sheetCount; i++ {
		builder.WriteString(fmt.Sprintf(`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" Target="worksheets/sheet%d.xml"/>`, i, i))
	}
	builder.WriteString(fmt.Sprintf(`<Relationship Id="rId%d" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" Target="styles.xml"/>`, sheetCount+1))
	builder.WriteString(`</Relationships>`)
	return builder.String()
}

func stylesXML() string {
	return `<?xml version="1.0" encoding="UTF-8" standalone="yes"?>` +
		`<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">` +
		`<fonts count="2"><font><sz val="11"/><name val="Calibri"/></font><font><b/><sz val="11"/><name val="Calibri"/></font></fonts>` +
		`<fills count="2"><fill><patternFill patternType="none"/></fill><fill><patternFill patternType="gray125"/></fill></fills>` +
		`<borders count="1"><border><left/><right/><top/><bottom/><diagonal/></border></borders>` +
		`<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>` +
		`<cellXfs count="2"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/><xf numFmtId="0" fontId="1" fillId="0" borderId="0" xfId="0" applyFont="1"/></cellXfs>` +
		`<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>` +
		`</styleSheet>`
}

func worksheetXML(rows [][]string) string {
	var builder strings.Builder
	builder.WriteString(`<?xml version="1.0" encoding="UTF-8" standalone="yes"?>`)
	builder.WriteString(`<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main"><sheetData>`)
	for rIdx, row := range rows {
		builder.WriteString(fmt.Sprintf(`<row r="%d">`, rIdx+1))
		for cIdx, cell := range row {
			ref := columnName(cIdx+1) + strconv.Itoa(rIdx+1)
			styleID := "0"
			if rIdx == 0 {
				styleID = "1"
			}
			builder.WriteString(fmt.Sprintf(`<c r="%s" t="inlineStr" s="%s"><is><t xml:space="preserve">%s</t></is></c>`, ref, styleID, xmlEscape(cell)))
		}
		builder.WriteString(`</row>`)
	}
	builder.WriteString(`</sheetData></worksheet>`)
	return builder.String()
}

func safeSheetName(name string, index int) string {
	replacer := strings.NewReplacer("\\", "_", "/", "_", "*", "_", "[", "_", "]", "_", ":", "_", "?", "_")
	name = strings.TrimSpace(replacer.Replace(name))
	if name == "" {
		name = fmt.Sprintf("Лист%d", index)
	}
	runes := []rune(name)
	if len(runes) > 31 {
		return string(runes[:31])
	}
	return name
}

func columnName(n int) string {
	if n <= 0 {
		return ""
	}
	var result []byte
	for n > 0 {
		n--
		result = append([]byte{byte('A' + n%26)}, result...)
		n /= 26
	}
	return string(result)
}

func xmlEscape(value string) string {
	var buffer bytes.Buffer
	_ = xml.EscapeText(&buffer, []byte(value))
	return buffer.String()
}

func upgradeToWebSocket(w http.ResponseWriter, r *http.Request) (io.ReadWriteCloser, error) {
	if !headerContainsToken(r.Header, "Connection", "upgrade") || !headerContainsToken(r.Header, "Upgrade", "websocket") {
		return nil, errors.New("ожидалось websocket upgrade соединение")
	}

	key := strings.TrimSpace(r.Header.Get("Sec-WebSocket-Key"))
	if key == "" {
		return nil, errors.New("отсутствует Sec-WebSocket-Key")
	}

	hijacker, ok := w.(http.Hijacker)
	if !ok {
		return nil, errors.New("server does not support hijacking")
	}

	conn, buf, err := hijacker.Hijack()
	if err != nil {
		return nil, fmt.Errorf("не удалось перехватить соединение: %w", err)
	}

	response := "HTTP/1.1 101 Switching Protocols\r\n" +
		"Upgrade: websocket\r\n" +
		"Connection: Upgrade\r\n" +
		"Sec-WebSocket-Accept: " + computeWebSocketAccept(key) + "\r\n\r\n"

	if _, err := buf.WriteString(response); err != nil {
		conn.Close()
		return nil, err
	}
	if err := buf.Flush(); err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func headerContainsToken(header http.Header, key, token string) bool {
	for _, value := range header.Values(key) {
		for _, part := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(part), token) {
				return true
			}
		}
	}
	return false
}

func computeWebSocketAccept(key string) string {
	sum := sha1.Sum([]byte(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"))
	return base64.StdEncoding.EncodeToString(sum[:])
}

type wsFrame struct {
	final   bool
	opcode  byte
	payload []byte
}

func readClientTextFrame(r io.Reader) ([]byte, error) {
	var message []byte
	var started bool

	for {
		frame, err := readClientFrame(r)
		if err != nil {
			return nil, err
		}

		switch frame.opcode {
		case 0x8:
			return nil, io.EOF
		case 0x9, 0xA:
			if !frame.final {
				return nil, errors.New("fragmented control websocket frames are not supported")
			}
			continue
		case 0x1:
			if started {
				return nil, errors.New("получено новое websocket сообщение до завершения предыдущего")
			}
			started = true
		case 0x0:
			if !started {
				return nil, errors.New("получен continuation websocket frame без начального текстового сообщения")
			}
		default:
			return nil, errors.New("поддерживаются только текстовые websocket сообщения")
		}

		if int64(len(message))+int64(len(frame.payload)) > 128*1024*1024 {
			return nil, errors.New("слишком большой websocket payload")
		}
		message = append(message, frame.payload...)

		if frame.final {
			return message, nil
		}
	}
}

func readClientFrame(r io.Reader) (wsFrame, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(r, header); err != nil {
		return wsFrame{}, err
	}

	final := header[0]&0x80 != 0
	opcode := header[0] & 0x0F
	if header[1]&0x80 == 0 {
		return wsFrame{}, errors.New("client websocket frame must be masked")
	}

	payloadLen := int64(header[1] & 0x7F)
	switch payloadLen {
	case 126:
		extended := make([]byte, 2)
		if _, err := io.ReadFull(r, extended); err != nil {
			return wsFrame{}, err
		}
		payloadLen = int64(binary.BigEndian.Uint16(extended))
	case 127:
		extended := make([]byte, 8)
		if _, err := io.ReadFull(r, extended); err != nil {
			return wsFrame{}, err
		}
		payloadLen = int64(binary.BigEndian.Uint64(extended))
	}

	if payloadLen > 128*1024*1024 {
		return wsFrame{}, errors.New("слишком большой websocket payload")
	}

	mask := make([]byte, 4)
	if _, err := io.ReadFull(r, mask); err != nil {
		return wsFrame{}, err
	}

	payload := make([]byte, payloadLen)
	if _, err := io.ReadFull(r, payload); err != nil {
		return wsFrame{}, err
	}

	for i := range payload {
		payload[i] ^= mask[i%4]
	}

	return wsFrame{
		final:   final,
		opcode:  opcode,
		payload: payload,
	}, nil
}

func writeServerJSON(w io.Writer, message wsMessage) error {
	payload, err := json.Marshal(message)
	if err != nil {
		return err
	}
	return writeServerTextFrame(w, payload)
}

func writeServerTextFrame(w io.Writer, payload []byte) error {
	header := []byte{0x81}
	length := len(payload)

	switch {
	case length <= 125:
		header = append(header, byte(length))
	case length <= 65535:
		header = append(header, 126, byte(length>>8), byte(length))
	default:
		extended := make([]byte, 8)
		binary.BigEndian.PutUint64(extended, uint64(length))
		header = append(header, 127)
		header = append(header, extended...)
	}

	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(payload)
	return err
}

func init() {
	log.SetOutput(os.Stdout)
}

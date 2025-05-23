package logger

import (
	"bytes"
	"fmt"
	"net/http"
	"os"
	"time"
)

const (
	FLAG_DEBUGP   = 0
	FLAG_TELEGRAM = 6
	FLAG_TRACE    = 5
	FLAG_DEBUG    = 4
	FLAG_INFO     = 3
	FLAG_WARN     = 2
	FLAG_ERROR    = 1

	Reset  = "\033[0m"
	Red    = "\033[31m"
	Green  = "\033[32m"
	Yellow = "\033[33m"
	Blue   = "\033[34m"
	Purple = "\033[35m"
	Cyan   = "\033[36m"
	Gray   = "\033[37m"
	White  = "\033[97m"
)

type LoggerConfig struct {
	Flag             int
	Identifier       string
	TelegramToken    string
	TelegramChatId   int
	TelegramThreadId uint
	Outputs          []*os.File
}

type Logger struct {
	Config *LoggerConfig
}

var config = &LoggerConfig{
	Flag:             FLAG_INFO,
	Outputs:          []*os.File{os.Stdout},
	TelegramChatId:   0,
	TelegramToken:    "",
	TelegramThreadId: 0, // Default
	// TelegramThreadId: 6010, // Devnet
	// TelegramThreadId: 6759, // Testnet
	// TelegramThreadId: 1, // worknet
}

var logger = &Logger{
	Config: config,
}

func SetConfig(newConfig *LoggerConfig) {
	config = newConfig
}

func SetOutputs(outputs []*os.File) {
	config.Outputs = outputs
}

func SetFlag(flag int) {

	config.Flag = flag
}

func SetTelegramInfo(token string, chatId int) {
	config.TelegramToken = token
	config.TelegramChatId = chatId
	config.TelegramThreadId = 0
}

func SetTelegramGroupInfo(token string, chatId int, threadId uint) {
	config.TelegramToken = token
	config.TelegramChatId = chatId
	config.TelegramThreadId = threadId
}

func SetIdentifier(identifier string) {
	config.Identifier = identifier
}

func DebugP(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_DEBUGP {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Purple, "DEBUG_P", message, a),
	)
}

func Trace(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_TRACE {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Blue, "TRACE", message, a),
	)
}

func Debug(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_DEBUG {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Cyan, "DEBUG", message, a),
	)
}

func Info(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_INFO {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Green, "INFO", message, a),
	)
}

func Warn(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_WARN {
		return
	}
	logger.writeToOutputs(
		getLogBuffer(Yellow, "WARN", message, a),
	)
}

func Error(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_ERROR {
		return
	}
	if config.TelegramToken != "" && config.TelegramChatId != 0 {
		sendToTelegram(getLogBuffer("", "ERROR", message, a))
	}
	logger.writeToError(getLogBuffer(Red, "ERROR", message, a))
}

func Telegram(message interface{}, a ...interface{}) {
	if config.Flag < FLAG_TELEGRAM {
		return
	}
	sendToTelegram(
		getLogBuffer("", "TELE", message, a),
	)
}

func sendToTelegram(message []byte) {
	var jsonStr []byte
	if config.TelegramThreadId > 0 {
		jsonStr = []byte(
			fmt.Sprintf(
				`{"chat_id": "%v", "message_thread_id": "%v", "text": "%v"}`,
				config.TelegramChatId,
				config.TelegramThreadId,
				string(message),
			),
		)
	} else {
		jsonStr = []byte(
			fmt.Sprintf(`{"chat_id": "%v", "text": "%v"}`, config.TelegramChatId, string(message)),
		)
	}
	resp, err := http.Post(
		fmt.Sprintf("https://api.telegram.org/bot%s/sendMessage", config.TelegramToken),
		"application/json",
		bytes.NewBuffer(jsonStr),
	)
	if err != nil {
		fmt.Println(err, resp)
	}
}

func getLogBuffer(color string, prefix string, message interface{}, a []interface{}) []byte {
	var buffer bytes.Buffer
	buffer.WriteString(color)
	if config.Identifier != "" {
		buffer.WriteString("[")
		buffer.WriteString(config.Identifier)
		buffer.WriteString("]")
	}
	buffer.WriteString("[")
	buffer.WriteString(prefix)
	buffer.WriteString("][")
	buffer.WriteString(time.Now().Format(time.Stamp))
	buffer.WriteString("] ")
	buffer.WriteString(fmt.Sprintf("%v ", message))
	for _, v := range a {
		buffer.WriteString(fmt.Sprintf("\n%v", v))
	}
	if color != "" {
		buffer.WriteString(Reset)
	}
	buffer.WriteString("\n")
	return buffer.Bytes()
}

func (logger *Logger) writeToOutputs(buffer []byte) {
	for i := range config.Outputs {
		config.Outputs[i].Write(buffer)
	}
}

func (logger *Logger) writeToError(buffer []byte) {
	os.Stderr.Write(buffer)
}

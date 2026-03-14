package errors

import (
	"encoding/json"
	"errors"
	"log"
	"runtime"
)

const (
	Critical = iota
	Notice
	Warning
	Ingnored
)

const DefaultSeverity = Notice

type ErrorInfo struct {
	Message       string         `json:"message"`
	Data          map[string]any `json:"data"`
	Err           error          `json:"err"`
	Stack         []CodeLocation `json:"stack"`
	BirthLocation *CodeLocation  `json:"birth_location"`
	Severity      int            `json:"severity"`
}

type CodeLocation struct {
	File     string `json:"file"`
	Line     int    `json:"line"`
	Function string `json:"function"`
}

func FromError(err error, msg string) *ErrorInfo {
	return &ErrorInfo{
		Message:       msg,
		Data:          make(map[string]any),
		Err:           err,
		Stack:         make([]CodeLocation, 0),
		BirthLocation: getCodeLocation(),
		Severity:      DefaultSeverity,
	}
}

func NewError(err string, msg string) *ErrorInfo {
	return FromError(errors.New(err), msg)
}

func Nil() *ErrorInfo {
	return &ErrorInfo{
		Message:       "nil",
		Data:          make(map[string]any),
		Err:           nil,
		Stack:         make([]CodeLocation, 0),
		BirthLocation: getCodeLocation(),
		Severity:      Ingnored,
	}
}

func getCodeLocation() *CodeLocation {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "unknown"
		line = 0
	}
	return &CodeLocation{
		File:     file,
		Line:     line,
		Function: runtime.FuncForPC(pc).Name(),
	}
}

func (e *ErrorInfo) PushStack() *ErrorInfo {
	e.Stack = append(e.Stack, *getCodeLocation())
	return e
}

func (e *ErrorInfo) WithSeverity(severity int) *ErrorInfo {
	e.Severity = severity
	return e
}

func (e *ErrorInfo) WithData(data map[string]any) *ErrorInfo {
	e.Data = data
	return e
}

func (e *ErrorInfo) IsNil() bool {
	return e == nil || e.Err == nil
}

func (e *ErrorInfo) Unwrap() error {
	return e.Err
}

func (e *ErrorInfo) Error() string {
	return e.JSON()
}

// MarshalJSON кастомная сериализация для ErrorInfo
func (e *ErrorInfo) MarshalJSON() ([]byte, error) {
	type Alias ErrorInfo
	var errMsg string
	if e.Err != nil {
		errMsg = e.Err.Error()
	}
	return json.Marshal(&struct {
		*Alias
		Err string `json:"err"`
	}{
		Alias: (*Alias)(e),
		Err:   errMsg,
	})
}

func (e *ErrorInfo) JSON() string {
	json, err := json.MarshalIndent(e, "", "  ")
	if err != nil {
		return ""
	}
	return string(json)
}

func (e *ErrorInfo) Fatal() {
	log.Fatal(e.Error())
}

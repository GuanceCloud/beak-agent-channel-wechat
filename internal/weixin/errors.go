package weixin

import (
	"errors"
	"fmt"
	"strings"
)

var ErrSessionExpired = errors.New("weixin session expired")

type SessionExpiredError struct {
	Operation string
	ErrCode   int
	ErrMsg    string
}

func NewSessionExpiredError(operation string, errCode int, errMsg string) error {
	return &SessionExpiredError{
		Operation: strings.TrimSpace(operation),
		ErrCode:   errCode,
		ErrMsg:    strings.TrimSpace(errMsg),
	}
}

func (e *SessionExpiredError) Error() string {
	if e == nil {
		return ErrSessionExpired.Error()
	}
	operation := e.Operation
	if operation == "" {
		operation = "ilink"
	}
	if e.ErrMsg == "" {
		return fmt.Sprintf("%s session expired: errcode=%d", operation, e.ErrCode)
	}
	return fmt.Sprintf("%s session expired: errcode=%d errmsg=%s", operation, e.ErrCode, e.ErrMsg)
}

func (e *SessionExpiredError) Unwrap() error {
	return ErrSessionExpired
}

func SessionExpiredInfo(err error) (operation string, errCode int, errMsg string, ok bool) {
	var expired *SessionExpiredError
	if !errors.As(err, &expired) || expired == nil {
		return "", 0, "", false
	}
	return expired.Operation, expired.ErrCode, expired.ErrMsg, true
}

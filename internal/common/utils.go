package common

import (
	"strings"

	"github.com/google/uuid"
)

func GenerateUUID() string {
	return uuid.New().String()
}

func MaskPasswordStdOut(password string) string {
	if len(password) <= 4 {
		return password
	}
	return password[:2] + strings.Repeat("*", len(password)-4) + password[len(password)-2:]
}

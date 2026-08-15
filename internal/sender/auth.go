package sender

import (
	"errors"
	"net/smtp"
	"strings"

	"mailbaby/internal/config"
)

type loginAuth struct {
	username string
	password string
	host     string
}

// LoginAuth returns an smtp.Auth that implements the SASL LOGIN mechanism.
// This is widely required by Chinese mail service providers (QQ, NetEase 163, Sina) and enterprise relays.
func LoginAuth(username, password, host string) smtp.Auth {
	return &loginAuth{
		username: username,
		password: password,
		host:     host,
	}
}

func (a *loginAuth) Start(server *smtp.ServerInfo) (string, []byte, error) {
	if !server.TLS && !isLocalhost(server.Name) {
		return "", nil, errors.New("unencrypted connection")
	}
	if server.Name != a.host {
		return "", nil, errors.New("wrong host name")
	}
	return "LOGIN", []byte(a.username), nil
}

func (a *loginAuth) Next(fromServer []byte, more bool) ([]byte, error) {
	if !more {
		return nil, nil
	}

	prompt := strings.ToLower(strings.TrimSpace(string(fromServer)))
	switch {
	case strings.Contains(prompt, "username") || strings.Contains(prompt, "user"):
		return []byte(a.username), nil
	case strings.Contains(prompt, "password") || strings.Contains(prompt, "pass"):
		return []byte(a.password), nil
	default:
		// Some servers send empty prompt for the password step
		return []byte(a.password), nil
	}
}

func isLocalhost(name string) bool {
	return name == "localhost" || name == "127.0.0.1" || name == "::1"
}

// BuildAuth selects and configures the appropriate smtp.Auth based on configuration.
func BuildAuth(authType config.SmtpAuthType, username, password, host string) smtp.Auth {
	if strings.TrimSpace(username) == "" && strings.TrimSpace(password) == "" {
		return nil
	}

	switch strings.ToUpper(string(authType)) {
	case "PLAIN":
		return smtp.PlainAuth("", username, password, host)
	case "LOGIN":
		return LoginAuth(username, password, host)
	case "CRAM-MD5":
		return smtp.CRAMMD5Auth(username, password)
	case "NONE":
		return nil
	case "", "AUTO":
		// Auto defaults to PlainAuth (standard RFC 4616)
		return smtp.PlainAuth("", username, password, host)
	default:
		return smtp.PlainAuth("", username, password, host)
	}
}

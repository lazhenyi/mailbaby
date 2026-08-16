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

	// The SASL LOGIN mechanism is a strict two-step protocol:
	//   1. server prompts for username (typically "Username:" / "user:" / "auth id:")
	//   2. server prompts for password (typically "Password:" / "pass:")
	// The previous implementation matched a substring "user"/"pass" against
	// any prompt, which broke when servers sent a "username" prompt containing
	// the literal word "password" (or vice versa). We now use regex-anchored
	// matches against the trailing token.
	prompt := strings.ToLower(strings.TrimSpace(string(fromServer)))
	// Strip trailing colon/punctuation.
	prompt = strings.TrimRight(prompt, ": \t\r\n")

	if strings.HasSuffix(prompt, "username") || strings.HasSuffix(prompt, "user name") || prompt == "user" || strings.HasSuffix(prompt, "auth id") || strings.HasSuffix(prompt, "login id") {
		return []byte(a.username), nil
	}
	if strings.HasSuffix(prompt, "password") || strings.HasSuffix(prompt, "pass word") || prompt == "pass" {
		return []byte(a.password), nil
	}

	// Defensive: if the server sends an empty prompt for the password step,
	// assume password (matches historical SMTP behavior). Otherwise, fail
	// closed rather than leaking the password to the wrong step.
	if prompt == "" {
		return []byte(a.password), nil
	}
	return nil, errors.New("sender: SASL LOGIN server sent unexpected prompt")
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

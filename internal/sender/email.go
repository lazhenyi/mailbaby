package sender

import (
	"encoding/json"
	"fmt"
	"mime"
	"net/mail"
	"path/filepath"
	"strings"
)

// Attachment represents a file attached to an email, either as a regular attachment or inline CID resource.
type Attachment struct {
	Filename    string `json:"filename"`
	ContentType string `json:"content_type"`
	Data        []byte `json:"data"`
	Inline      bool   `json:"inline"`
	ContentID   string `json:"content_id,omitempty"` // for <img src="cid:xyz">
}

// Email represents an email message to be sent via the SMTP subsystem.
type Email struct {
	ID          string            `json:"id,omitempty"`
	Account     string            `json:"account,omitempty"` // Target SMTP account (empty for default)
	From        string            `json:"from,omitempty"`    // Override account default From
	FromName    string            `json:"from_name,omitempty"`
	ReplyTo     string            `json:"reply_to,omitempty"`
	To          []string          `json:"to"`
	Cc          []string          `json:"cc,omitempty"`
	Bcc         []string          `json:"bcc,omitempty"`
	Subject     string            `json:"subject"`
	TextBody    string            `json:"text_body,omitempty"`
	HTMLBody    string            `json:"html_body,omitempty"`
	Headers     map[string]string `json:"headers,omitempty"`
	Attachments []*Attachment     `json:"attachments,omitempty"`
	Tags        []string          `json:"tags,omitempty"`
	Metadata    map[string]string `json:"metadata,omitempty"`
}

// NewEmail creates an initialized Email struct.
func NewEmail() *Email {
	return &Email{
		Headers:     make(map[string]string),
		Metadata:    make(map[string]string),
		Attachments: make([]*Attachment, 0),
	}
}

// SetAccount sets the target SMTP account name for sending.
func (e *Email) SetAccount(account string) *Email {
	e.Account = strings.TrimSpace(account)
	return e
}

// SetFrom sets the sender's email address and optional display name.
func (e *Email) SetFrom(from string, name ...string) *Email {
	e.From = strings.TrimSpace(from)
	if len(name) > 0 {
		e.FromName = strings.TrimSpace(name[0])
	}
	return e
}

// SetReplyTo sets the Reply-To address.
func (e *Email) SetReplyTo(replyTo string) *Email {
	e.ReplyTo = strings.TrimSpace(replyTo)
	return e
}

// AddTo adds one or more primary recipients.
func (e *Email) AddTo(addresses ...string) *Email {
	for _, addr := range addresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			e.To = append(e.To, trimmed)
		}
	}
	return e
}

// AddCc adds one or more carbon-copy recipients.
func (e *Email) AddCc(addresses ...string) *Email {
	for _, addr := range addresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			e.Cc = append(e.Cc, trimmed)
		}
	}
	return e
}

// AddBcc adds one or more blind carbon-copy recipients.
func (e *Email) AddBcc(addresses ...string) *Email {
	for _, addr := range addresses {
		trimmed := strings.TrimSpace(addr)
		if trimmed != "" {
			e.Bcc = append(e.Bcc, trimmed)
		}
	}
	return e
}

// SetSubject sets the email subject line.
func (e *Email) SetSubject(subject string) *Email {
	e.Subject = subject
	return e
}

// SetTextBody sets the plain-text body content.
func (e *Email) SetTextBody(body string) *Email {
	e.TextBody = body
	return e
}

// SetHTMLBody sets the rich HTML body content.
func (e *Email) SetHTMLBody(body string) *Email {
	e.HTMLBody = body
	return e
}

// SetHeader sets a custom MIME header.
func (e *Email) SetHeader(key, value string) *Email {
	if e.Headers == nil {
		e.Headers = make(map[string]string)
	}
	e.Headers[key] = value
	return e
}

// Attach attaches a file to the email.
func (e *Email) Attach(filename string, data []byte, contentType ...string) *Email {
	ct := detectContentType(filename, contentType...)
	e.Attachments = append(e.Attachments, &Attachment{
		Filename:    filename,
		ContentType: ct,
		Data:        data,
		Inline:      false,
	})
	return e
}

// AttachInline attaches an inline file (e.g. image with Content-ID for HTML <img src="cid:xxx">).
func (e *Email) AttachInline(filename, contentID string, data []byte, contentType ...string) *Email {
	ct := detectContentType(filename, contentType...)
	e.Attachments = append(e.Attachments, &Attachment{
		Filename:    filename,
		ContentType: ct,
		Data:        data,
		Inline:      true,
		ContentID:   strings.Trim(contentID, "<>"),
	})
	return e
}

// AllRecipients returns a consolidated list of all unique recipient email addresses (To + Cc + Bcc).
func (e *Email) AllRecipients() []string {
	seen := make(map[string]struct{})
	var result []string

	add := func(list []string) {
		for _, addr := range list {
			parsed, err := mail.ParseAddress(addr)
			clean := addr
			if err == nil && parsed != nil {
				clean = parsed.Address
			}
			clean = strings.TrimSpace(clean)
			if clean == "" {
				continue
			}
			lower := strings.ToLower(clean)
			if _, exists := seen[lower]; !exists {
				seen[lower] = struct{}{}
				result = append(result, clean)
			}
		}
	}

	add(e.To)
	add(e.Cc)
	add(e.Bcc)
	return result
}

// Validate validates that the email has essential fields before sending.
func (e *Email) Validate() error {
	if e == nil {
		return ErrNilEmail
	}

	if len(e.To) == 0 && len(e.Cc) == 0 && len(e.Bcc) == 0 {
		return ErrNoRecipients
	}

	if e.From != "" {
		if _, err := mail.ParseAddress(e.From); err != nil {
			return fmt.Errorf("%w: %q (%v)", ErrInvalidFrom, e.From, err)
		}
	}

	validateAddresses := func(list []string, field string) error {
		for _, addr := range list {
			if _, err := mail.ParseAddress(addr); err != nil {
				return fmt.Errorf("%w in %s: %q (%v)", ErrInvalidRecipient, field, addr, err)
			}
		}
		return nil
	}

	if err := validateAddresses(e.To, "To"); err != nil {
		return err
	}
	if err := validateAddresses(e.Cc, "Cc"); err != nil {
		return err
	}
	if err := validateAddresses(e.Bcc, "Bcc"); err != nil {
		return err
	}

	if e.ReplyTo != "" {
		if _, err := mail.ParseAddress(e.ReplyTo); err != nil {
			return fmt.Errorf("sender: invalid reply_to email %q: %w", e.ReplyTo, err)
		}
	}

	return nil
}

// ToJSON serializes the email to JSON format.
func (e *Email) ToJSON() ([]byte, error) {
	return json.Marshal(e)
}

// FromJSON deserializes JSON data into the email struct.
func (e *Email) FromJSON(data []byte) error {
	return json.Unmarshal(data, e)
}

func detectContentType(filename string, custom ...string) string {
	if len(custom) > 0 && strings.TrimSpace(custom[0]) != "" {
		return custom[0]
	}
	ext := filepath.Ext(filename)
	if ext != "" {
		mimeType := mime.TypeByExtension(ext)
		if mimeType != "" {
			return mimeType
		}
	}
	return "application/octet-stream"
}

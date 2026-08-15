package sender

import (
	"bytes"
	"crypto/rand"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"mime"
	"mime/multipart"
	"net/textproto"
	"os"
	"regexp"
	"strings"
	"time"
)

// headerFieldNameRe matches RFC 5322 field-name: printable US-ASCII
// excluding colon (obs-ftext: %d33-57 / %d59-126).
var headerFieldNameRe = regexp.MustCompile(`^[\x21-\x39\x3B-\x7E]+$`)

// validHeaderFieldName reports whether s is a safe RFC 5322 header field name.
func validHeaderFieldName(s string) bool {
	return headerFieldNameRe.MatchString(s)
}

// sanitizeHeaderValue strips CR/LF so no header value can inject new fields.
func sanitizeHeaderValue(v string) string {
	if !strings.ContainsAny(v, "\r\n") {
		return v
	}
	var sb strings.Builder
	for _, r := range v {
		if r == '\r' || r == '\n' {
			continue
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

// BuildMIME compiles an Email struct into an RFC 5322 MIME-compliant byte slice.
func BuildMIME(email *Email, defaultFrom, defaultFromName string) ([]byte, error) {
	var buf bytes.Buffer

	// 1. Determine From and FromName
	fromAddr := email.From
	if fromAddr == "" {
		fromAddr = defaultFrom
	}
	fromName := email.FromName
	if fromName == "" {
		fromName = defaultFromName
	}

	fromHeader := formatAddress(fromName, fromAddr)
	buf.WriteString("From: " + fromHeader + "\r\n")

	// 2. Add To
	if len(email.To) > 0 {
		buf.WriteString("To: " + strings.Join(email.To, ", ") + "\r\n")
	}

	// 3. Add Cc
	if len(email.Cc) > 0 {
		buf.WriteString("Cc: " + strings.Join(email.Cc, ", ") + "\r\n")
	}

	// 4. Add Reply-To
	if email.ReplyTo != "" {
		buf.WriteString("Reply-To: " + email.ReplyTo + "\r\n")
	}

	// 5. Add Date
	buf.WriteString("Date: " + time.Now().Format(time.RFC1123Z) + "\r\n")

	// 6. Add Subject (RFC 2047 encoded)
	buf.WriteString("Subject: " + encodeHeader(email.Subject) + "\r\n")

	// 7. Add Message-ID if not provided
	hasMsgID := false
	for k := range email.Headers {
		if strings.EqualFold(k, "Message-ID") {
			hasMsgID = true
			break
		}
	}
	if !hasMsgID {
		host, _ := os.Hostname()
		if host == "" {
			host = "mailbaby.local"
		}
		buf.WriteString(fmt.Sprintf("Message-ID: <%s@%s>\r\n", generateRandomID(), host))
	}

	// 8. Add MIME-Version
	buf.WriteString("MIME-Version: 1.0\r\n")

	// 9. Custom Headers
	for k, v := range email.Headers {
		if !validHeaderFieldName(k) {
			return nil, fmt.Errorf("sender: invalid custom header name %q", k)
		}
		if strings.EqualFold(k, "From") || strings.EqualFold(k, "To") ||
			strings.EqualFold(k, "Cc") || strings.EqualFold(k, "Bcc") ||
			strings.EqualFold(k, "Subject") || strings.EqualFold(k, "Date") ||
			strings.EqualFold(k, "Reply-To") ||
			strings.EqualFold(k, "MIME-Version") || strings.EqualFold(k, "Content-Type") {
			continue
		}
		buf.WriteString(fmt.Sprintf("%s: %s\r\n", k, sanitizeHeaderValue(encodeHeader(v))))
	}

	// 10. Split attachments into regular and inline
	var regularAttachments []*Attachment
	var inlineAttachments []*Attachment
	for _, att := range email.Attachments {
		if att.Inline {
			inlineAttachments = append(inlineAttachments, att)
		} else {
			regularAttachments = append(regularAttachments, att)
		}
	}

	// Case A: Has regular attachments -> outermost is multipart/mixed
	if len(regularAttachments) > 0 {
		mixedWriter := multipart.NewWriter(&buf)
		buf.WriteString(fmt.Sprintf("Content-Type: multipart/mixed; boundary=%q\r\n\r\n", mixedWriter.Boundary()))

		// Write body part (which could be related, alternative, or simple text/html)
		bodyPartHeader := make(textproto.MIMEHeader)
		if len(inlineAttachments) > 0 {
			relWriter := multipart.NewWriter(bytes.NewBuffer(nil))
			bodyPartHeader.Set("Content-Type", fmt.Sprintf("multipart/related; boundary=%q", relWriter.Boundary()))
			part, err := mixedWriter.CreatePart(bodyPartHeader)
			if err != nil {
				return nil, err
			}
			if err := writeRelatedBody(part, email, inlineAttachments, relWriter.Boundary()); err != nil {
				return nil, err
			}
		} else {
			if err := writeAlternativeOrSingle(mixedWriter, email); err != nil {
				return nil, err
			}
		}

		// Write regular attachments
		for _, att := range regularAttachments {
			if err := writeAttachmentPart(mixedWriter, att); err != nil {
				return nil, err
			}
		}

		if err := mixedWriter.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	// Case B: Has inline attachments but no regular attachments -> outermost is multipart/related
	if len(inlineAttachments) > 0 {
		relWriter := multipart.NewWriter(&buf)
		buf.WriteString(fmt.Sprintf("Content-Type: multipart/related; boundary=%q\r\n\r\n", relWriter.Boundary()))

		if err := writeRelatedBody(&buf, email, inlineAttachments, relWriter.Boundary()); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	// Case C: No attachments at all
	if email.TextBody != "" && email.HTMLBody != "" {
		// multipart/alternative
		altWriter := multipart.NewWriter(&buf)
		buf.WriteString(fmt.Sprintf("Content-Type: multipart/alternative; boundary=%q\r\n\r\n", altWriter.Boundary()))
		if err := writeAlternativeBodyParts(altWriter, email); err != nil {
			return nil, err
		}
		if err := altWriter.Close(); err != nil {
			return nil, err
		}
		return buf.Bytes(), nil
	}

	if email.HTMLBody != "" {
		buf.WriteString("Content-Type: text/html; charset=UTF-8\r\n")
		buf.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
		buf.WriteString(wrapBase64([]byte(email.HTMLBody)))
		buf.WriteString("\r\n")
		return buf.Bytes(), nil
	}

	// Default fallback to text/plain
	buf.WriteString("Content-Type: text/plain; charset=UTF-8\r\n")
	buf.WriteString("Content-Transfer-Encoding: base64\r\n\r\n")
	buf.WriteString(wrapBase64([]byte(email.TextBody)))
	buf.WriteString("\r\n")
	return buf.Bytes(), nil
}

func writeRelatedBody(w ioWriter, email *Email, inline []*Attachment, boundary string) error {
	relWriter := multipart.NewWriter(w)
	if err := relWriter.SetBoundary(boundary); err != nil {
		// SetBoundary might fail if boundary format is invalid, but boundary is generated by multipart.NewWriter
	}

	// 1. Text/HTML alternative body
	if email.TextBody != "" && email.HTMLBody != "" {
		altHeader := make(textproto.MIMEHeader)
		altWriter := multipart.NewWriter(bytes.NewBuffer(nil))
		altHeader.Set("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", altWriter.Boundary()))
		part, err := relWriter.CreatePart(altHeader)
		if err != nil {
			return err
		}
		subAltWriter := multipart.NewWriter(part)
		if err := subAltWriter.SetBoundary(altWriter.Boundary()); err != nil {
			return err
		}
		if err := writeAlternativeBodyParts(subAltWriter, email); err != nil {
			return err
		}
		if err := subAltWriter.Close(); err != nil {
			return err
		}
	} else if email.HTMLBody != "" {
		htmlHeader := make(textproto.MIMEHeader)
		htmlHeader.Set("Content-Type", "text/html; charset=UTF-8")
		htmlHeader.Set("Content-Transfer-Encoding", "base64")
		part, err := relWriter.CreatePart(htmlHeader)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte(wrapBase64([]byte(email.HTMLBody)) + "\r\n"))
	} else {
		textHeader := make(textproto.MIMEHeader)
		textHeader.Set("Content-Type", "text/plain; charset=UTF-8")
		textHeader.Set("Content-Transfer-Encoding", "base64")
		part, err := relWriter.CreatePart(textHeader)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte(wrapBase64([]byte(email.TextBody)) + "\r\n"))
	}

	// 2. Inline images/attachments
	for _, att := range inline {
		header := make(textproto.MIMEHeader)
		header.Set("Content-Type", fmt.Sprintf("%s; name=%q", att.ContentType, encodeHeader(att.Filename)))
		header.Set("Content-Transfer-Encoding", "base64")
		header.Set("Content-Disposition", fmt.Sprintf("inline; filename=%q", encodeHeader(att.Filename)))
		header.Set("Content-ID", fmt.Sprintf("<%s>", att.ContentID))

		part, err := relWriter.CreatePart(header)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte(wrapBase64(att.Data) + "\r\n"))
	}

	return relWriter.Close()
}

func writeAlternativeOrSingle(parent *multipart.Writer, email *Email) error {
	if email.TextBody != "" && email.HTMLBody != "" {
		altHeader := make(textproto.MIMEHeader)
		altWriter := multipart.NewWriter(bytes.NewBuffer(nil))
		altHeader.Set("Content-Type", fmt.Sprintf("multipart/alternative; boundary=%q", altWriter.Boundary()))
		part, err := parent.CreatePart(altHeader)
		if err != nil {
			return err
		}
		subAlt := multipart.NewWriter(part)
		if err := subAlt.SetBoundary(altWriter.Boundary()); err != nil {
			return err
		}
		if err := writeAlternativeBodyParts(subAlt, email); err != nil {
			return err
		}
		return subAlt.Close()
	}

	if email.HTMLBody != "" {
		h := make(textproto.MIMEHeader)
		h.Set("Content-Type", "text/html; charset=UTF-8")
		h.Set("Content-Transfer-Encoding", "base64")
		part, err := parent.CreatePart(h)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte(wrapBase64([]byte(email.HTMLBody)) + "\r\n"))
		return nil
	}

	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", "text/plain; charset=UTF-8")
	h.Set("Content-Transfer-Encoding", "base64")
	part, err := parent.CreatePart(h)
	if err != nil {
		return err
	}
	_, _ = part.Write([]byte(wrapBase64([]byte(email.TextBody)) + "\r\n"))
	return nil
}

func writeAlternativeBodyParts(w *multipart.Writer, email *Email) error {
	if email.TextBody != "" {
		textHeader := make(textproto.MIMEHeader)
		textHeader.Set("Content-Type", "text/plain; charset=UTF-8")
		textHeader.Set("Content-Transfer-Encoding", "base64")
		part, err := w.CreatePart(textHeader)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte(wrapBase64([]byte(email.TextBody)) + "\r\n"))
	}

	if email.HTMLBody != "" {
		htmlHeader := make(textproto.MIMEHeader)
		htmlHeader.Set("Content-Type", "text/html; charset=UTF-8")
		htmlHeader.Set("Content-Transfer-Encoding", "base64")
		part, err := w.CreatePart(htmlHeader)
		if err != nil {
			return err
		}
		_, _ = part.Write([]byte(wrapBase64([]byte(email.HTMLBody)) + "\r\n"))
	}

	return nil
}

func writeAttachmentPart(w *multipart.Writer, att *Attachment) error {
	header := make(textproto.MIMEHeader)
	header.Set("Content-Type", fmt.Sprintf("%s; name=%q", att.ContentType, encodeHeader(att.Filename)))
	header.Set("Content-Transfer-Encoding", "base64")
	header.Set("Content-Disposition", fmt.Sprintf("attachment; filename=%q", encodeHeader(att.Filename)))

	part, err := w.CreatePart(header)
	if err != nil {
		return err
	}

	_, err = part.Write([]byte(wrapBase64(att.Data) + "\r\n"))
	return err
}

type ioWriter interface {
	Write(p []byte) (n int, err error)
}

func formatAddress(name, address string) string {
	if name == "" {
		return address
	}
	encodedName := encodeHeader(name)
	return fmt.Sprintf("%s <%s>", encodedName, address)
}

func encodeHeader(s string) string {
	// If all ASCII and printable, return directly
	isASCII := true
	for i := 0; i < len(s); i++ {
		if s[i] > 127 || s[i] < 32 && s[i] != '\t' {
			isASCII = false
			break
		}
	}
	if isASCII {
		return s
	}
	return mime.BEncoding.Encode("UTF-8", s)
}

func wrapBase64(data []byte) string {
	encoded := base64.StdEncoding.EncodeToString(data)
	const lineLen = 76
	if len(encoded) <= lineLen {
		return encoded
	}

	var sb strings.Builder
	for i := 0; i < len(encoded); i += lineLen {
		end := i + lineLen
		if end > len(encoded) {
			end = len(encoded)
		}
		sb.WriteString(encoded[i:end])
		sb.WriteString("\r\n")
	}
	return strings.TrimRight(sb.String(), "\r\n")
}

func generateRandomID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

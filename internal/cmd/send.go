package cmd

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"time"

	"mailbaby/internal/config"
	"mailbaby/internal/sender"
)

// runSend provides a CLI command to send an immediate test email message.
func runSend(cfg *config.Config, args []string) error {
	fs := flag.NewFlagSet("send", flag.ContinueOnError)

	var to, subject, textBody, htmlBody, account, from string
	fs.StringVar(&to, "to", "", "Recipient email address (required)")
	fs.StringVar(&subject, "subject", "MailBaby Test Email", "Email subject line")
	fs.StringVar(&textBody, "body", "This is a test email sent via MailBaby CLI.", "Plain text email body")
	fs.StringVar(&htmlBody, "html", "", "Optional HTML body")
	fs.StringVar(&account, "account", "default", "SMTP account name to use")
	fs.StringVar(&from, "from", "", "Optional custom sender address override")

	if err := fs.Parse(args); err != nil {
		return err
	}

	if to == "" {
		return errors.New("cmd: --to recipient address is required")
	}

	mailSender, err := sender.NewFromConfig(cfg)
	if err != nil {
		return fmt.Errorf("cmd: failed to initialize sender: %w", err)
	}
	defer mailSender.Close()

	email := sender.NewEmail().
		SetAccount(account).
		AddTo(to).
		SetSubject(subject).
		SetTextBody(textBody)

	if htmlBody != "" {
		email.SetHTMLBody(htmlBody)
	}
	if from != "" {
		email.SetFrom(from)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Printf("[INFO] Sending email to <%s> via account %q...\n", to, account)
	start := time.Now()
	if err := mailSender.Send(ctx, email); err != nil {
		return fmt.Errorf("cmd: delivery failed: %w", err)
	}

	fmt.Printf("[SUCCESS] Email successfully sent in %v!\n", time.Since(start))
	return nil
}

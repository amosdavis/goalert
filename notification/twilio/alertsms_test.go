package twilio

import (
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/target/goalert/notification"
)

func resultCheck(t *testing.T, expected string, res string, err error) {
	t.Helper()
	t.Log("Length", len(res))
	assert.NoError(t, err)
	assert.Equal(t, expected, res)
}

func TestMapGSM(t *testing.T) {
	check := func(input, exp string) {
		t.Run("", func(t *testing.T) {
			res := strings.Map(mapGSM, input)
			if res != exp {
				t.Errorf("got %s; want %s", strconv.Quote(res), strconv.Quote(exp))
			}
		})
	}

	check("foo\\bar", "foo?bar")
	check("foo\bar", "fooar")
	check("foo\nbar", "foo bar")
	check("foo\t bar@/ok:asdf", "foo  bar@/ok:asdf")
	check("[Testing] {alert_message: `okay`}", "(Testing) (alert-message: 'okay')")
}

func TestSMS_RenderAlert(t *testing.T) {
	check := func(name string, a notification.Alert, link string, code int, exp string) {
		t.Run(name, func(t *testing.T) {
			res, err := renderAlertMessage("TestApp", a, link, code)
			resultCheck(t, exp, res, err)
		})
	}

	check("normal",
		notification.Alert{
			AlertID: 123,
			Summary: "Testing",
		},
		"https://example.com/alerts/123",
		1,
		`TestApp: Alert #123: Testing

https://example.com/alerts/123`,
	)

	check("no-reply-code",
		notification.Alert{
			AlertID: 123,
			Summary: "Testing",
		},
		"https://example.com/alerts/123",
		0,
		`TestApp: Alert #123: Testing

https://example.com/alerts/123`,
	)

	check("no-link",
		notification.Alert{
			AlertID: 123,
			Summary: "Testing",
		},
		"",
		1,
		`TestApp: Alert #123: Testing`,
	)

	check("no-link-or-reply-code",
		notification.Alert{
			AlertID: 123,
			Summary: "Testing",
		},
		"",
		0,
		`TestApp: Alert #123: Testing`,
	)

	check("truncate",
		notification.Alert{
			AlertID: 123,
			Summary: "Testing with a really really obnoxiously long message that will be need to be truncated at some point.",
		},
		"https://example.com/alerts/123",
		1,
		`TestApp: Alert #123: Testing with a really really obnoxiously long message that will be need to be truncated at some point.

https://example.com/alerts/123`,
	)

	check("truncate-long-id",
		notification.Alert{
			AlertID: 123456789,
			Summary: "Testing with a really really obnoxiously long message that will be need to be truncated at some point.",
		},
		"https://example.com/alerts/123",
		1,
		`TestApp: Alert #123456789: Testing with a really really obnoxiously long message that will be need to be truncated at some point

https://example.com/alerts/123`,
	)

	check("message-too-long",
		// can't fit summary
		notification.Alert{
			AlertID: 123456789,
			Summary: "Testing with a really really obnoxiously long message that will be need to be truncated at some point.",
		},
		"https://example.com/alerts/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff",
		123456789,
		`TestApp: Alert #123456789: Testing with a really really obnoxiousl

https://example.com/alerts/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff/123ff`,
	)
}

func TestSMS_RenderAlertBundle(t *testing.T) {
	check := func(name string, a notification.AlertBundle, link string, code int, exp string) {
		t.Run(name, func(t *testing.T) {
			res, err := renderAlertBundleMessage("TestApp", a, link, code)
			resultCheck(t, exp, res, err)
		})
	}

	check("alert-bundle-one",
		notification.AlertBundle{
			Count:       1,
			ServiceName: "My Service",
		},
		"https://example.com/services/321-654/alerts",
		100,
		"TestApp: Svc 'My Service': 1 unacked alert\n\n\thttps://example.com/services/321-654/alerts\n",
	)

	check("alert-bundle",
		notification.AlertBundle{
			Count:       5,
			ServiceName: "My Service",
		},
		"https://example.com/services/321-654/alerts",
		100,
		"TestApp: Svc 'My Service': 5 unacked alerts\n\n\thttps://example.com/services/321-654/alerts\n",
	)
}

func TestSMS_RenderAlertStatus(t *testing.T) {
	check := func(name string, a notification.AlertStatus, exp string) {
		t.Run(name, func(t *testing.T) {
			res, err := renderAlertStatusMessage("TestApp", a)
			resultCheck(t, exp, res, err)
		})
	}

	check("alert-status",
		notification.AlertStatus{
			AlertID:  123,
			Summary:  "Testing",
			LogEntry: "Some log entry",
		},
		`TestApp: Alert #123: Testing

	Some log entry`,
	)
}

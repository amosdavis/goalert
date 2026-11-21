package objstore

import "fmt"

// A SettingProblem is a storage setting that breaks or weakens the Delta
// log (docs/filestore-failure-modes.md, F34), as found by a store's
// CheckSettings.
type SettingProblem struct {
	// Breaks means the table cannot work safely with the setting, so
	// GoAlert must not start. Otherwise it is a warning, such as a cost.
	Breaks  bool
	Setting string
	Problem string
}

func (p SettingProblem) String() string {
	kind := "warning"
	if p.Breaks {
		kind = "breaks the table"
	}
	return fmt.Sprintf("%s (%s): %s", p.Setting, kind, p.Problem)
}

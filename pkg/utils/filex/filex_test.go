package filex

import "testing"

func TestDeleteFiles(t *testing.T) {
	err := DeleteFiles("./*.txt")
	t.Log(err)
}

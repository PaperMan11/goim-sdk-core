package command

import (
	"bytes"
	"os/exec"
)

func ExecCmdOut(cmdStr string) (ret string, err error) {
	//return exec.Command("/bin/sh", "-c", cmd).Output()
	//return exec.Command("/bin/bash", "-c", cmdStr).CombinedOutput()
	cmd := exec.Command("/bin/bash", "-c", cmdStr)
	var out bytes.Buffer
	var stderr bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &stderr
	err = cmd.Run()
	if err != nil {
		ret = stderr.String()
		return
	}
	ret = out.String()
	return
}

package service

type DittoCommand string

func (cmd DittoCommand) String() string {
	return string(cmd)
}

const (
	MODIFY_THING DittoCommand = "/things/twin/commands/modify"
)

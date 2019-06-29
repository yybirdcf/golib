package queue

type Queue interface {
	Push(string, string) error
	Run()
	Close()
}

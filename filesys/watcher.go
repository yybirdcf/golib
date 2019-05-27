package filesys

import "github.com/fsnotify/fsnotify"

type EventHanler func(*fsnotify.Event)

type ErrorHanler func(error)

type FsWatcher interface {
	Init(EventHanler, ErrorHanler) error
	Run()
	Add(string) error
	Remove(string) error
}

type fsnotifyWatcher struct {
	watcher      *fsnotify.Watcher
	eventHandler EventHanler
	errorHanler  ErrorHanler
}

func NewFsNotifyWatcher() {
	return &fsnotifyWatcher{}
}

func (f *fsnotifyWatcher) Init(eventHanler EventHanler, errorHanler ErrorHanler) error {
	f.watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	f.eventHandler = eventHanler
	f.errorHanler = errorHanler
}

func (f *fsnotifyWatcher) Add(path string) error {
	return f.watcher.Add(path)
}

func (f *fsnotifyWatcher) Remove(path string) error {
	return f.watcher.Remove(path)
}

func (f *fsnotifyWatcher) Run() {
	go func() {
		defer f.watcher.Close()

		for {
			select {
			case evt := <- f.watcher.Events:
				if f.eventHandler != nil {
					f.eventHandler(evt)
				}
			case err := <- f.watcher.Errors:
				if f.errorHanler != nil {
					f.errorHanler(err)
				}
			}
		}
	}
}

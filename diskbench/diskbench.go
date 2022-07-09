package diskbench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dbx123/go-progress/progress"
	"github.com/dbx123/go-utils/fileutils"
	"github.com/dbx123/go-utils/timeutils"
)

const (
	DiskBenchNumLines = 10000
	FolderName        = "bench"
)

var (
	start time.Time
)

type DiskBenchResult struct {
	Performed bool
	Path      string
	NumLines  int
	Writes    int
	JobName   string
	Duration  time.Duration
}

type DiskBench struct {
	Folder  string
	Seconds int
}

func (r DiskBenchResult) String() string {
	return fmt.Sprintf("\t\t\t"+"Performed: %v"+"\n"+
		"\t\t\t"+"Path: %v"+"\n"+
		"\t\t\t"+"NumLines: %v"+"\n"+
		"\t\t\t"+"Writes: %v",
		r.Performed,
		r.Path,
		r.NumLines,
		r.Writes,
	)
}

func BenchDisk(d DiskBench) (DiskBenchResult, error) {

	var jobName string = "Benchmarking Disk"
	var took time.Duration
	var absPath string

	if d.Seconds < 1 {
		return DiskBenchResult{}, fmt.Errorf("seconds cannot be less than 1 [%v]", d.Seconds)
	}

	var targetFolder string = fmt.Sprintf("%v/%v", d.Folder, FolderName)

	if !fileutils.FolderExists(targetFolder) {
		err := fileutils.MkDir(targetFolder)
		if err != nil {
			return DiskBenchResult{}, fmt.Errorf("unable to create folder %v", targetFolder)
		}
	}

	absPath, err := filepath.Abs(targetFolder)
	if err != nil {
		return DiskBenchResult{}, err
	}

	tSeq, took := sequential(d)

	// brutally remove the target folder
	os.Remove(targetFolder)

	return DiskBenchResult{
		Performed: true,
		Path:      absPath,
		NumLines:  DiskBenchNumLines,
		Writes:    tSeq,
		JobName:   jobName,
		Duration:  took,
	}, nil
}

func sequential(bench DiskBench) (int, time.Duration) {

	var jobName string = "Sequential Writes"

	start = time.Now()
	var took time.Duration
	var duration time.Duration = time.Second * time.Duration(bench.Seconds)
	var filesWritten int

	var taskNames []string
	for i := 1; i <= int(bench.Seconds); i++ {
		taskNames = append(taskNames, strconv.Itoa(i))
	}

	// Setup the job for this process
	job := progress.SetupJob(jobName,
		taskNames,
		func(taskName string) float64 { return 1 },
	)
	defer job.End(true)

	var condition bool = true

	for ok := true; ok; ok = condition {

		condition = time.Since(start) < duration

		elapsed := time.Since(start)

		f := fmt.Sprintf("%v/%v.txt", bench.Folder, filesWritten)
		d := fileutils.GetFile(f)
		err := writeLines(d, duration)
		if err != nil {
			d.Close()
			condition = false
		}
		d.Close()
		filesWritten++

		sPassed := int(elapsed.Seconds())
		for i := 1; i < int(duration.Seconds()); i++ {
			if sPassed >= i {
				task, _ := job.GetTask(strconv.Itoa(i))
				if task.StartTime == nil {
					task.Start()
				}
				if task.Took == nil {
					task.End()
				}
				nTask, _ := job.GetTask(strconv.Itoa(i + 1))
				if nTask.StartTime == nil {
					nTask.Start()
				}
			}
		}
		job.UpdateBar()
	}

	nTask, _ := job.GetTask(strconv.Itoa(bench.Seconds))
	if nTask.Took == nil {
		nTask.End()
	}

	took = timeutils.Took(start)

	return filesWritten, took
}

func writeLines(f *os.File, duration time.Duration) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{}, 1)

	go func(d *os.File, ctx context.Context) {

		for i := 0; i <= DiskBenchNumLines; i++ {

			if _, err := d.WriteString(fmt.Sprintf("%v\n", time.Now().UnixNano())); err != nil {
				return
			}

			select {
			default:
				if i == DiskBenchNumLines {
					done <- struct{}{}
				}
			case <-ctx.Done():
				return
			}
		}

	}(f, ctx)

	end := time.Until(start.Add(duration))

	select {
	case <-done:
		return nil
	case <-time.After(end):
		return ctx.Err()
	}
}

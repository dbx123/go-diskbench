package diskbench

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/dbx123/go-logger/logger"
	"github.com/dbx123/go-progress/progress"
	"github.com/dbx123/go-utils/fileutils"
	"github.com/dbx123/go-utils/timeutils"
)

const (
	SecondsToRun      = 5
	DiskBenchNumLines = 10000
)

var (
	start time.Time
)

type DiskBenchResult struct {
	Performed bool
	Path      string
	NumLines  int
	Writes    int
}

func (r DiskBenchResult) String() string {
	return fmt.Sprintf("\t\t\t"+"Performed: %v"+"\n"+
		"\t\t\t"+"Path: %v"+"\n"+
		"\t\t\t"+"NumLines: %v"+"\n"+
		"\t\t\t"+"SequentialWrites: %v",
		r.Performed,
		r.Path,
		r.NumLines,
		r.Writes,
	)
}

func BenchDisk(folder string) DiskBenchResult {

	var jobName string = "Benchmarking Disk"

	var start time.Time = time.Now()
	var took time.Duration
	var absPath string

	absPath, err := filepath.Abs(folder)
	if err != nil {
		panic(err)
	}

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("%v [path: %v]\n", jobName, absPath),
	)

	outFolder := fmt.Sprintf("%s/bench", folder)

	fileutils.EmptyFolder(outFolder)
	tSeq := sequential(outFolder)

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", jobName, took),
	)

	return DiskBenchResult{
		Performed: true,
		Path:      absPath,
		NumLines:  DiskBenchNumLines,
		Writes:    tSeq,
	}
}

func sequential(outFolder string) int {

	var jobName string = "Sequential Writes"

	start = time.Now()
	var took time.Duration
	var magnitude int = SecondsToRun
	var duration time.Duration = time.Second * SecondsToRun
	var filesWritten int

	logger.Log(
		logger.LVL_APP,
		fmt.Sprintf("%v [seconds: %v]\n", jobName, magnitude),
	)

	var taskNames []string
	for i := 1; i <= SecondsToRun; i++ {
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

		f := fmt.Sprintf("%v/%v.txt", outFolder, filesWritten)
		d := fileutils.GetFile(f)
		err := writeLines(d)
		if err != nil {
			d.Close()
			condition = false
		}
		d.Close()
		filesWritten++

		sPassed := int(elapsed.Seconds())
		for i := 1; i < SecondsToRun; i++ {
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
	nTask, _ := job.GetTask(strconv.Itoa(SecondsToRun))
	if nTask.Took == nil {
		nTask.End()
	}

	took = timeutils.Took(start)
	logger.Log(
		logger.LVL_DEBUG,
		fmt.Sprintf("Done %v [%v]\n", jobName, took),
	)

	return filesWritten
}

func writeLines(f *os.File) error {
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

	end := time.Until(start.Add(time.Second * SecondsToRun))

	select {
	case <-done:
		return nil
	case <-time.After(end):
		return ctx.Err()
	}
}

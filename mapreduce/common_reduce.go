package mapreduce

import (
	"encoding/json"
	"fmt"
	"os"
	//"sync"
)

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// TODO:
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//messages := make(chan KeyValue, 20)

	taskdic := make(map[string][]string)
	var err error
	inlist := make([]InHandle, nMap)

	//var wg sync.WaitGroup 
	for i := 0; i < nMap; i = i+1 {
		inlist[i].f, err = os.Open(reduceName(jobName, i, reduceTaskNumber))
		defer (inlist[i].f).Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Reduce_Work Open mapfile Error: %s\n", err)
			panic(err.Error())
		}
		
		/*wg.Add(1)
		go func(dec *json.Decoder) {
			defer wg.Done()
			for dec.More() {
				var m KeyValue
				err := dec.Decode(&m)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Reduce_Work Decode message Error: %s\n", err)
					panic(err.Error())
				}
				messages <- m
			}
		}(json.NewDecoder(inlist[i].f))*/

		inlist[i].dec = json.NewDecoder(inlist[i].f)
		for (inlist[i].dec).More() {
			var m KeyValue
			err := (inlist[i].dec).Decode(&m)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Reduce_Work Decode message Error: %s\n", err)
				panic(err.Error())
			}
			taskdic[m.Key] = append(taskdic[m.Key], m.Value)
		}
	}

	// closer
	/*go func() {
		wg.Wait()
		close(messages)
	}()

	for mes := range messages {
		taskdic[mes.Key] = append(taskdic[mes.Key], mes.Value)
	}*/

	resultfile, err := os.Create(mergeName(jobName, reduceTaskNumber))
	defer resultfile.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Reduce_Work create resultfile Error: %s\n", err)
		panic(err.Error())
	}
	enc := json.NewEncoder(resultfile)
	for key, v := range taskdic {
		err = enc.Encode(KeyValue{key, reduceF(key, v)})
		if err != nil {
			fmt.Fprintf(os.Stderr, "Reduce_Work josn encode Error: %s\n", err)
			panic(err.Error())
		}
	}
}

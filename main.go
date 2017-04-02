//Test project for appengine
package galileo2

import (
	"fmt"
	"math/rand"
	"net/http"
	"time"
	"strconv"
	"strings"

	"golang.org/x/net/context"

	"google.golang.org/appengine"
	"google.golang.org/appengine/datastore"
	"google.golang.org/appengine/log"
	"google.golang.org/appengine/taskqueue"
)

const (
	//Each cron delete run can process up to max KeysOnly
	MAX_KEYS_ONLY_RECS = 50
	//Each request to delete can contain up to this number of records
	REC_PER_DEL_JOB = 10
	//Each run of delete cron will only produce up to this number of processes
	MAX_JOBS_TO_BE_RUN = 100
)

type ActivityLog struct {
	Timestamp time.Time
	Component string
	Message   string `datastore:",noindex"`
}

func init() {
	http.HandleFunc("/", root)
	http.HandleFunc("/cron", handleCron)
	http.HandleFunc("/helper", handleHelper)
}

func root(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	fmt.Fprintf(w, "<h1>ActivityLog Cron Delete</h1>")
	fmt.Fprintf(w, "<hr>")
	fmt.Fprintf(w, "<h3>Test Data</h3>")
	fmt.Fprintf(w, "<hr>")
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=insert_origins\">Insert ActivityLog for ProductOrigin [+10]</a> [<a href=\"/helper?f=insert_origins&n=100\">+100</a>] [<a href=\"/helper?f=insert_origins&n=1000\">+1000</a>] [<a href=\"/helper?f=insert_origins&n=10000\">+10000</a>] [<a href=\"/helper?f=insert_origins&n=100000\">+100000</a>] [<a href=\"/helper?f=insert_origins&n=200000\">+200000</a>] [<a href=\"/helper?f=insert_origins&n=300000\">+300000</a>]</h5>")
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=insert_destinations\">Insert ActivityLog for ProductDestination [+10]</a> [<a href=\"/helper?f=insert_destinations&n=100\">+100</a>] [<a href=\"/helper?f=insert_destinations&n=1000\">+1000</a>] [<a href=\"/helper?f=insert_destinations&n=10000\">+10000</a>] [<a href=\"/helper?f=insert_destinations&n=100000\">+100000</a>] [<a href=\"/helper?f=insert_destinations&n=200000\">+200000</a>] [<a href=\"/helper?f=insert_destinations&n=300000\">+300000</a>]</h5>")
	fmt.Fprintf(w, "<hr>")

	fmt.Fprintf(w, "<h3>Status</h3>")
	q := datastore.NewQuery("ActivityLog")
	recCount, _ := q.Count(c)

	fmt.Fprintf(w, "<h5>ActivityLog Records: %v records</h5>", recCount)

	t := time.Now().Local()
	TIMESTAMP := t.AddDate(0, 0, -7)
	q = datastore.NewQuery("ActivityLog").Filter("Timestamp <=", TIMESTAMP)
	recCount, _ = q.Count(c)

	fmt.Fprintf(w, "<h5>Old Records: %v records</h5>", recCount)
	fmt.Fprintf(w, "<hr>")
	fmt.Fprintf(w, "<h3>Test Deletion Manually</h3>")
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=debug_daily_delete_old_act_logs\">Manual Execute Delete Cron Jobs</a></h5>")
	fmt.Fprintf(w, "<hr>")

	fmt.Fprintf(w, "<h3>Reset Table</h3>")
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=reset\">Delete ActivityLog All Rows</a></h5>")
	fmt.Fprintf(w, "<hr>")
}

func handleCron(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	C_FUNC := r.FormValue("f")

	switch C_FUNC {
	case "hourly_monitor_entity":
		//simulate hourly insert of activity logs
		t := taskqueue.NewPOSTTask("/helper", map[string][]string{"f": {"insert_origins"}})
		if _, err := taskqueue.Add(c, t, ""); err != nil {
			panic(err)
			return
		}

		t = taskqueue.NewPOSTTask("/helper", map[string][]string{"f": {"insert_destinations"}})
		if _, err := taskqueue.Add(c, t, ""); err != nil {
			panic(err)
			return
		}

	case "daily_delete_old_act_logs":
		//simulate daily logs deletion
		//to follow
		//same as in the debug code//
		//--------------------------
		deleteAllOldRecords(c,w)
		
	case "delete_records":
		keyString := r.FormValue("k")
		batchid := r.FormValue("id")
		log.Infof(c, "\n\nBATCH: %v CRON: Deleting record keys: %v", batchid, keyString)
		
		//Prepare delete
		//BATCH: 2017-03-26 21:49:57.1142708 +0800 SGT/3 CRON: Deleting record keys: ###/ActivityLog,ProductOrigin/ActivityLog,6658642417811456###/ActivityLog,ProductOrigin/ActivityLog,5743848743501824###/ActivityLog,ProductOrigin/ActivityLog,5040161301725184###/ActivityLog,ProductOrigin/ActivityLog,6166061208567808###/ActivityLog,ProductOrigin/ActivityLog,6359575255056384###/ActivityLog,ProductOrigin/ActivityLog,5163306604036096###/ActivityLog,ProductDestination/ActivityLog,5409597208657920###/ActivityLog,ProductDestination/ActivityLog,6324390882967552###/ActivityLog,ProductDestination/ActivityLog,5479965952835584###/ActivityLog,ProductDestination/ActivityLog,5620703441190912
		SPL := strings.Split(keyString, "###")
		for _, id := range SPL {
			if id != "" {
				tid := parseID(id)
				if tid == 0 {
					log.Errorf(c, "\n\nBATCH: %v CRON: Error in key: %v", batchid, id)
				}
				key := datastore.NewKey(c, "ActivityLog", "", tid, nil)
				err := datastore.Delete(c, key)
				if err != nil {
					log.Errorf(c, "\n\nBATCH: %v CRON: DELETE ERROR: %v", batchid, key)
				} else {
					log.Infof(c, "\n\nBATCH: %v CRON: DELETE SUCCESS: %v", batchid, key)
				}
			}
		}
		
	default:
		fmt.Fprintf(w, "<h1>handler: handleCron</h1>")

	}

}

func insertDatastoreRec(c context.Context, insChan chan string, parent, component string) {

	activityLog := generateRandomActivityLogs(c, component)
	_, id, err := activityLog.Insert(c, parent)
	if err != nil {
		log.Infof(c, "INSERT ERROR: %v", err)
		insChan <- fmt.Sprintf("ERROR: %v", err)
	} else {
		log.Infof(c, "INSERT SUCCESS: %v", id)
		insChan <- fmt.Sprintf("SUCCESS: %v", id)
	}
}

func handleHelper(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)

	H_FUNC := r.FormValue("f")

	switch H_FUNC {
	case "insert_origins":
		rows := r.FormValue("n")
		if rows == "" {
			rows = "10"
		}
		nRows, _ := strconv.Atoi(rows)
		for i := 0; i <= nRows; i++ {
			insChan := make(chan string)
			go insertDatastoreRec(c,insChan,"ProductOrigin","PriceMonitor")
			fmt.Fprintf(w, "Inserted ID: %v<br> STATUS: %v", i, <-insChan)
			
		}

		for i := 0; i <= nRows; i++ {
			insChan := make(chan string)
			go insertDatastoreRec(c,insChan,"ProductOrigin","Repricer")
			fmt.Fprintf(w, "Inserted ID: %v<br> STATUS: %v", i, <-insChan)
		}

	case "insert_destinations":
		rows := r.FormValue("n")
		if rows == "" {
			rows = "10"
		}
		nRows, _ := strconv.Atoi(rows)
		for i := 0; i <= nRows; i++ {
			insChan := make(chan string)
			go insertDatastoreRec(c,insChan,"ProductDestination","PriceMonitor")
			fmt.Fprintf(w, "Inserted ID: %v<br> STATUS: %v", i, <-insChan)
		}

		for i := 0; i <= nRows; i++ {
			insChan := make(chan string)
			go insertDatastoreRec(c,insChan,"ProductDestination","Repricer")
			fmt.Fprintf(w, "Inserted ID: %v<br> STATUS: %v", i, <-insChan)
		}

	//temp cron delete debugger
	case "debug_daily_delete_old_act_logs":
		/* 			%% - CronDeleteManager will first determine the number of records to be deleted (do it by x batch of records)
		   			%% - Define settings how many records each job in parallel will process (say 1,000 records)
		   			%% - Key here is to reduce runtime for all transactions probably each job must run below 1,000ms to reduce cost
		   			%% - Each cron will process a set of keys to be processed
		   			%% - Goroutines will be used to execute datastore.MultiDelete
					%% - The taskqueues will be run separately for deleting each batch of keys
		*/
		deleteAllOldRecords(c,w)
		
	case "reset":
		resetTable(c)
		log.Infof(c, "Reset done!")
		fmt.Fprintf(w, "Reset done!")

	}
}

func deleteAllOldRecords(c context.Context, w http.ResponseWriter) {

	fmt.Fprintf(w, "Deleting all old records!<hr>")

	//while old activity logs exists
	recCount := 1
	attempt := 0
	max_reached := false
	batchStamp := ""
	
	for recCount > 0 && max_reached == false {

		//Target records older than 7 days from now
		t := time.Now().Local()
		TIMESTAMP := t.AddDate(0, 0, -7)

		q := datastore.NewQuery("ActivityLog").Filter("Timestamp <=", TIMESTAMP)
		recCount, _ = q.Count(c)
		attempt++
		batchStamp = fmt.Sprintf("%v", TIMESTAMP)
		//temp
		if attempt > 5 {
			log.Infof(c, "break: exceeded max processes")
			recCount = 0
			break
		}

		fmt.Fprintf(w, "LOOP [%v] -=> saw [%v] old records<br>", attempt, recCount)

		//Deleting records that are 7 days older
		q = datastore.NewQuery("ActivityLog").
			Filter("Timestamp <= ", TIMESTAMP).
			KeysOnly().
			Limit(MAX_KEYS_ONLY_RECS) //make sure we dont process all old records at once!
		recCount, _ = q.Count(c)
		fmt.Fprintf(w, "For this batch, we will process [%v] old records<br>", recCount)
		
		keys, err := q.GetAll(c, nil)
		if err != nil {
			panic(err)
		}
		if err != nil {
			fmt.Fprintf(w, "Error: %v<br>", err)
			return
		}

		//Each key is like below
		///ActivityLog,ProductOrigin/ActivityLog,6342962321555456
		//Trigger process to delete these records

		delChan := make(chan bool)
		goRoutine := 0

		for i := 0; i < len(keys); i += REC_PER_DEL_JOB {
			end := i + REC_PER_DEL_JOB

			if end > len(keys) {
				end = len(keys)
			}

			goRoutine++
			//Delete each chunk
			fmt.Fprintf(w, "[GO-ROUTINE]# %v<br>", goRoutine)

			//run batch multidelete via goroutine
			go deleteBatchMulti(c, delChan, keys[i:end], fmt.Sprintf("%v/%v", batchStamp, goRoutine))
			<-delChan
			if goRoutine > MAX_JOBS_TO_BE_RUN {
				//dont run so much taskqueue jobs
				log.Infof(c, "MAX_JOBS_TO_BE_RUN limit has been reached!")
				max_reached = true
				break
			}

		}

	}
	if attempt >= 1 {
		fmt.Fprintf(w, "<hr>LOOP TOTAL: %v<br>", attempt)
	}
}

func deleteBatchMulti(c context.Context, delChan chan bool, keys []*datastore.Key, batchID string) {
	//pass keys as string
	thisKeys := ""
	for _, key := range keys {
		thisKeys = fmt.Sprintf("%v###%v", thisKeys, key)
	}
	t := taskqueue.NewPOSTTask("/cron?f=delete_records", map[string][]string{"k": {thisKeys}, "id": {batchID}})
	if _, err := taskqueue.Add(c, t, ""); err != nil {
		panic(err)
		return
	}
	delChan <- true
	
	
}

func resetTable(c context.Context) {
	//Deleting records that are 7 days older
	q := datastore.NewQuery("ActivityLog").KeysOnly()
	recCount, _ := q.Count(c)
	log.Infof(c, "resetTable: [%v] records deleted", recCount)
	
	keys, err := q.GetAll(c, nil)
	if err != nil {
		log.Errorf(c, "resetTable: ERROR: %v ", err)
		panic(err)
	}
 	err = datastore.DeleteMulti(c, keys)
	if err != nil {
		log.Errorf(c, "resetTable: ERROR: %v ", err)
		panic(err)
	}
}

func randNum(min, max int) int {
	rand.Seed(time.Now().UTC().UnixNano())
	return rand.Intn(max-min) + min
}

func parseID(keyStr string) (id int64) {
	///ActivityLog,ProductDestination/ActivityLog,5633897580724224
	
	SPL := strings.Split(keyStr, ",")
	if len(SPL) >= 3 {
		id, err := strconv.ParseInt(SPL[2], 10, 64)
		if err != nil {
			return id
		}
		return id	
	}
	return id
}

func generateRandomActivityLogs(c context.Context, forType string) *ActivityLog {

	t := time.Now().Local()
	ranInt := randNum(1, 10)

	al := new(ActivityLog)
	al.Timestamp = t.AddDate(0, 0, -ranInt)
	al.Component = forType
	al.Message = fmt.Sprintf("Sample activity log random %v", ranInt)

	return al
}

func defaultActivityLog(c context.Context, forAns string) *datastore.Key {
	return datastore.NewKey(c, "ActivityLog", forAns, 0, nil)
}

func (t *ActivityLog) Key(c context.Context, forAns string) *datastore.Key {
	return datastore.NewIncompleteKey(c, "ActivityLog", defaultActivityLog(c, forAns))
}

func (t *ActivityLog) Insert(c context.Context, forAns string) (al *ActivityLog, id int64, err error) {
	k, err := datastore.Put(c, t.Key(c, forAns), t)
	if err != nil {
		return nil, int64(0), err
	}
	return t, k.IntID(), nil
}

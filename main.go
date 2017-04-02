//Test project for appengine
package galileo

import (
	"net/http"
	"math/rand"
	"time"
	"fmt"
	"appengine/datastore"
	"appengine"
	"appengine/taskqueue"
)

const (
	//Each request to delete can contain up to this number of records
    REC_PER_DEL_JOB = 10
	//Each run of delete cron will only produce up to this number of processes
	MAX_JOBS_TO_BE_RUN = 100
)
 
type ActivityLog struct {
	Timestamp 	time.Time
	Component 	string
	Message 	string `datastore:",noindex"`
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
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=insert_origins\">Insert ActivityLog for ProductOrigin</a></h5>")
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=insert_destinations\">Insert ActivityLog for ProductDestination</a></h5>")
	fmt.Fprintf(w, "<hr>")
	
	fmt.Fprintf(w, "<h3>Status</h3>")
	q := datastore.NewQuery("ActivityLog")
	recCount,_ := q.Count(c)
	
	fmt.Fprintf(w, "<h5>ActivityLog Records: %v records</h5>", recCount)
	
	t := time.Now().Local()
	TIMESTAMP := t.AddDate(0,0,-7)	
	q = datastore.NewQuery("ActivityLog").Filter("Timestamp <=", TIMESTAMP)
	recCount,_ = q.Count(c)
	
	fmt.Fprintf(w, "<h5>Old Records: %v records</h5>", recCount)
	fmt.Fprintf(w, "<hr>")
	fmt.Fprintf(w, "<h3>Test Deletion Manually</h3>")
	fmt.Fprintf(w, "<h5><a href=\"/helper?f=debug_daily_delete_old_act_logs\">Manual Execute Delete Cron Jobs</a></h5>")
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
			c.Infof("[CRON] Deleting all old records!<hr>")
			
			//while old activity logs exists
			recCount := 1
			attempt := 0
			max_reached := false
			
			for recCount > 0 && max_reached == false {
				
				//Target records older than 7 days from now
				t := time.Now().Local()
				TIMESTAMP := t.AddDate(0,0,-7)	
				
				q := datastore.NewQuery("ActivityLog").Filter("Timestamp <=", TIMESTAMP)
				recCount,_ = q.Count(c)
				attempt++
				//temp
				if attempt > 5 {
					c.Infof("break: exceeded max processes")
					recCount = 0
				}
				
				c.Infof("LOOP [%v] -=> saw [%v] old records<br>", attempt, recCount)		
				
				//Deleting records that are 7 days older
				q = datastore.NewQuery("ActivityLog").
									 Filter("Timestamp <= ", TIMESTAMP).
									 KeysOnly()
									 
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
					c.Infof("[GO-ROUTINE]# %v<br>", goRoutine)
					
					//run batch multidelete via goroutine
					go deleteBatchMulti(c,delChan,keys[i:end])
					<-delChan					
					if goRoutine > MAX_JOBS_TO_BE_RUN  {
						//dont run so much taskqueue jobs
						c.Infof("MAX_JOBS_TO_BE_RUN!")
						max_reached = true
						break
					}
					
				}
				
			}
			if attempt >= 1 {
				c.Infof("<hr>LOOP TOTAL: %v<br>", attempt)
			}

		
		default:
			fmt.Fprintf(w, "<h1>handler: handleCron</h1>")
		
	}

}

func handleHelper(w http.ResponseWriter, r *http.Request) {
	c := appengine.NewContext(r)
	
	H_FUNC := r.FormValue("f")
	
	switch H_FUNC {
		case "insert_origins":
			for i:=0; i<=10; i++ {
				activityLog := generateRandomActivityLogs(c,"PriceMonitor")
				aclog, id, err := activityLog.Insert(c,"ProductOrigin")
				if err != nil {
					fmt.Fprintf(w, "Error: %v<br>", err)
					fmt.Fprintf(w, "Data: %v<br>", aclog)
				} else {
					fmt.Fprintf(w, "Inserted ID: %v<br>", id)
				}
			}
			
			for i:=0; i<=10; i++ {
				activityLog := generateRandomActivityLogs(c,"Repricer")
				aclog, id, err := activityLog.Insert(c,"ProductOrigin")
				if err != nil {
					fmt.Fprintf(w, "Error: %v<br>", err)
					fmt.Fprintf(w, "Data: %v<br>", aclog)
				} else {
					fmt.Fprintf(w, "Inserted ID: %v<br>", id)
				}
			}			
			
		case "insert_destinations":
			for i:=0; i<=10; i++ {
				activityLog := generateRandomActivityLogs(c,"PriceMonitor")
				aclog, id, err := activityLog.Insert(c,"ProductDestination")
				if err != nil {
					fmt.Fprintf(w, "Error: %v<br>", err)
					fmt.Fprintf(w, "Data: %v<br>", aclog)
				} else {
					fmt.Fprintf(w, "Inserted ID: %v<br>", id)
				}
			}
			
			for i:=0; i<=10; i++ {
				activityLog := generateRandomActivityLogs(c,"Repricer")
				aclog, id, err := activityLog.Insert(c,"ProductDestination")
				if err != nil {
					fmt.Fprintf(w, "Error: %v<br>", err)
					fmt.Fprintf(w, "Data: %v<br>", aclog)
				} else {
					fmt.Fprintf(w, "Inserted ID: %v<br>", id)
				}
			}
			
		//temp cron delete debugger
		case "debug_daily_delete_old_act_logs":
/* 			%% - CronDeleteManager will first determine the number of records to be deleted (do it by x batch of records)
			%% - Define settings how many records each job in parallel will process (say 1,000 records)
			%% - Key here is to reduce runtime for all transactions probably each job must run below 1,000ms to reduce cost
			%% - Each cron will process a set of keys to be processed
			%% - Goroutines will be used to execute datastore.MultiDelete		
 */			
			fmt.Fprintf(w, "Deleting all old records!<hr>")
			
			//while old activity logs exists
			recCount := 1
			attempt := 0
			max_reached := false
			
			for recCount > 0 && max_reached == false {
				
				//Target records older than 7 days from now
				t := time.Now().Local()
				TIMESTAMP := t.AddDate(0,0,-7)	
				
				q := datastore.NewQuery("ActivityLog").Filter("Timestamp <=", TIMESTAMP)
				recCount,_ = q.Count(c)
				attempt++
				//temp
				if attempt > 5 {
					c.Infof("break: exceeded max processes")
					recCount = 0
				}
				
				fmt.Fprintf(w, "LOOP [%v] -=> saw [%v] old records<br>", attempt, recCount)		
				
				//Deleting records that are 7 days older
				q = datastore.NewQuery("ActivityLog").
									 Filter("Timestamp <= ", TIMESTAMP).
									 KeysOnly()
									 
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
					go deleteBatchMulti(c,delChan,keys[i:end])
					<-delChan					
					if goRoutine > MAX_JOBS_TO_BE_RUN  {
						//dont run so much taskqueue jobs
						c.Infof("MAX_JOBS_TO_BE_RUN!")
						max_reached = true
						break
					}
					
				}
				
			}
			if attempt >= 1 {
				fmt.Fprintf(w, "<hr>LOOP TOTAL: %v<br>", attempt)
			}
		
	}
}

func randNum(min, max int) int {
	rand.Seed(time.Now().UTC().UnixNano())
    return rand.Intn(max - min) + min
}

func deleteBatchMulti(c appengine.Context,delChan chan bool,keys []*datastore.Key) {
	err := datastore.DeleteMulti(c, keys)
	if err != nil {
		panic(err)
	}
	delChan <- true
}

func generateRandomActivityLogs(c appengine.Context, forType string) (*ActivityLog) {
	
	t := time.Now().Local()
	ranInt := randNum(1,10)
	
	al := new(ActivityLog)
	al.Timestamp		= t.AddDate(0,0,-ranInt)
	al.Component		= forType
	al.Message			= fmt.Sprintf("Sample activity log random %v", ranInt)
	
	return al
}


func defaultActivityLog(c appengine.Context, forAns string) *datastore.Key {
	return datastore.NewKey(c, "ActivityLog", forAns, 0, nil)
}
 
func (t *ActivityLog) Key(c appengine.Context, forAns string) *datastore.Key {
	return datastore.NewIncompleteKey(c, "ActivityLog", defaultActivityLog(c,forAns))
}
 
func (t *ActivityLog) Insert(c appengine.Context, forAns string) (al *ActivityLog, id int64, err error) {
	k, err := datastore.Put(c, t.Key(c,forAns), t)
	if err != nil {
		return nil, int64(0), err
	}
	return t, k.IntID(), nil
}

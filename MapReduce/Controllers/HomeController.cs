using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Web.Mvc;

namespace MapReduce.Controllers
{
    public class HomeController : Controller
    {

        // Total jobs to run
        private static readonly int TOTAL_JOBS = Convert.ToInt32(ConfigurationManager.AppSettings["TOTAL_JOBS"]);

        // Size of each Job
        private static readonly int JOB_SIZE = Convert.ToInt32(ConfigurationManager.AppSettings["JOB_SIZE"]);

        // Starting number to test
        private static readonly int START_VALUE = Convert.ToInt32(ConfigurationManager.AppSettings["START_VALUE"]);

        // Used for locking
        private static object LockObject = new object();

        // Job storage
        private static Stack<Job> JobsToSend { get; set; }
        private static List<Job> PendingJobs { get; set; }
        private static List<Job> CompletedJobs { get; set; }

        // WaitOne() is Lock and Set() is release 
        // Why? Because someone at microsoft who has a short supply of brain cells,
        // thought it would be a good idea to name things with no concern for convention
        private static AutoResetEvent ThreadLock { get; set; }

        /// <summary>
        /// Show main page
        /// </summary>
        /// <returns>main page</returns>
        public ActionResult Index()
        {
            ViewBag.Title = "Home Page";

            return View();
        }

        /// <summary>
        /// Main compute function
        /// </summary>
        /// <returns>html list of prime numbers</returns>
        [HttpPost]
        public JsonResult Compute()
        {

            DateTime start = DateTime.Now;
            // Reset all variables
            Init();

            // Block until complete
            Lock();


            // Get Primes
            string primes = "";
            foreach (int prime in CompletedJobs.AsParallel()
                .Where(j => j.JobType == JobType.REDUCE)
                .Select(j => Job.ListToDict(j.Keys, j.Values, true))
                .Select(dict => dict.Keys).SelectMany(l => l))
            {
                primes += prime.ToString() + "</br>";
            }


            // Compute Meta Data
            string metaData = "";
            
            int jobsRun = CompletedJobs.Count();
            
            double avgJobTime = new TimeSpan(Convert.ToInt64(CompletedJobs.Select(j => j.EndTime - j.StartTime)
                .Average(timeSpan => timeSpan.Ticks)))
                .TotalMilliseconds / 1000d;

            // Get list of all nodes and how many jobs each node completed
            ConcurrentDictionary<string,int> nodeJobCount = new ConcurrentDictionary<string, int>();
            ConcurrentDictionary<string, List<TimeSpan>> nodeJobTimes = new ConcurrentDictionary<string, List<TimeSpan>>();
            Parallel.ForEach(CompletedJobs, job => 
            {
                nodeJobCount.AddOrUpdate(job.NodeID, 1, (k, v) => ++v);
                nodeJobTimes.AddOrUpdate(job.NodeID, new List<TimeSpan>() { (job.EndTime - job.StartTime) }, (k, v) => 
                    {
                        v.Add((job.EndTime - job.StartTime));
                        return v;
                    });
            });

            double totalTime = (DateTime.Now - start).TotalMilliseconds / 1000d;

            double machineTime = totalTime + avgJobTime * jobsRun;

            metaData += "Total Jobs Run: " + jobsRun + "</br>" +
                        "Average Job Time: " + avgJobTime + "s</br>" +
                        "Server Time: " + totalTime + "s</br>" +
                        "Total Machine Time: " + machineTime + "s</br>" + 
                        "Packet Size: " + (36 + 5 * JOB_SIZE) + "bytes</br>" +
                        "Total Bytes Transferred : " + ((36 + 5 * JOB_SIZE) * jobsRun) + "bytes</br>" +
                        "<u>Nodes : Job Count : Avg Job Time</u></br>";
            for(int i = 0; i < nodeJobCount.Count(); i++)
            {
                string key = nodeJobCount.Keys.ToList()[i];
                metaData += key + " : " + nodeJobCount[key] + " : " + 
                    new TimeSpan(Convert.ToInt64(nodeJobTimes[key].Average(timeSpan => timeSpan.Ticks))).TotalMilliseconds / 1000d + "s</br>";
            }


            // Convert to json and return
            object returnObj = new { data = metaData, answer = primes };

            return Json(returnObj, JsonRequestBehavior.DenyGet);
        }

        #region NodeAPI

        /// <summary>
        /// Returns jobs for mapping, reducing and waiting
        /// </summary>
        /// <returns>Job for node</returns>
        public JsonResult GetJob()
        {
            // If no jobs are available wait
            if (JobsToSend == null)
            {
                return Json(new Job(JobType.NULL, null), JsonRequestBehavior.AllowGet);
            }

            lock (LockObject)
            {
                // If job is available send it
                if(JobsToSend.Count() > 0)
                {
                    Job jobToSend = JobsToSend.Pop();
                    if (!PendingJobs.Contains(jobToSend))
                    {
                        PendingJobs.Add(jobToSend);
                    }
                    return Json(jobToSend, JsonRequestBehavior.AllowGet);
                }

                // If waiting for jobs to complete, send out duplicates to possibly improve runtime
                if(PendingJobs.Count() > 0)
                {
                    foreach(Job job in PendingJobs)
                    {
                        JobsToSend.Push(job);
                    }
                    return Json(JobsToSend.Pop(), JsonRequestBehavior.AllowGet);
                }
            }
            
            // If all jobs are complete and nothing is being processed, wait
            return Json(new Job(JobType.NULL, null), JsonRequestBehavior.AllowGet);

        }

        /// <summary>
        /// Collect Job data
        /// </summary>
        /// <param name="recieved">Job recieved</param>
        public void PostJob(Job recieved)
        {
            lock (LockObject)
            {
                // If not a duplicate (or is the first to return of a duplicate set)
                if(PendingJobs.Where(j => j.ID == recieved.ID).Count() > 0)
                {
                    PendingJobs.RemoveAll(j => j.ID == recieved.ID);
                    CompletedJobs.Add(recieved);

                    // If a map job and it is a pseudo prime, then add reduce job for it
                    if(recieved.JobType == JobType.MAP)
                    {
                        JobsToSend.Push(new Job(JobType.REDUCE, Job.ListToDict(recieved.Keys, recieved.Values, true)));
                    }
                }
            }

            // If all out of jobs to run, then mapreduce is finished
            if (PendingJobs.Count() == 0 && JobsToSend.Count() == 0)
            {
                Unlock();
            }
        }

        #endregion

        /// <summary>
        /// Initalizes all variables
        /// </summary>
        private void Init()
        {

            JobsToSend = new Stack<Job>();
            PendingJobs = new List<Job>();
            CompletedJobs = new List<Job>();

            GenerateJobs();
        }

        /// <summary>
        /// Adds all needed jobs to list
        /// </summary>
        private void GenerateJobs()
        {
            for(int i = 0; i < TOTAL_JOBS; i++)
            {
                Dictionary<int, bool> payload = new Dictionary<int, bool>();
                for(int j = 0; j < JOB_SIZE; j++)
                {
                    payload.Add(START_VALUE + (i * JOB_SIZE) + j, false);
                }
                JobsToSend.Push(new Job(JobType.MAP, payload));
            }
        }

        #region MutexControl

        // Block
        private void Lock()
        {
            if (ThreadLock == null)
            {
                ThreadLock = new AutoResetEvent(false);
            }
            ThreadLock.WaitOne();
            ThreadLock = null;
        }

        // Unblock
        private void Unlock()
        {
            if(ThreadLock != null)
            {
                ThreadLock.Set();
            }
        }

        #endregion

    }

    public enum JobType {NULL = 0, MAP, REDUCE, EXIT }

    public class Job
    {
        private static int Global_Job_Counter = 0;

        public JobType JobType { get; set; }
        public List<int> Keys { get; set; }
        public List<bool> Values { get; set; }
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public int ID { get; set; }
        public string NodeID { get; set; }


        /// <summary>
        /// Used for creating real jobs
        /// </summary>
        /// <param name="job">What job type</param>
        /// <param name="payload">What the payload is (prime or pseduo prime number)</param>
        public Job(JobType job, Dictionary<int,bool> payload)
        {
            JobType = job;
            ID = Global_Job_Counter++;
            if(payload != null)
            {
                Keys = payload.Keys.ToList();
                Values = payload.Values.ToList();
            }
        }

        public static Dictionary<int,bool> ListToDict(List<int> k, List<bool> v, bool filter)
        {
            Dictionary<int, bool> payload = new Dictionary<int, bool>();

            if (k == null || v == null)
            {
                return payload;
            }


            for (int i = 0; i < k.Count(); i++)
            {
                if ((filter && v[i]) || !filter)
                {
                    payload.Add(k[i], v[i]);
                }
            }

            return payload;
        }

        /// <summary>
        /// Used because JSON to C# doesn't know how to parse strings without being stupid
        /// </summary>
        public Job()
        {

        }
    }
}

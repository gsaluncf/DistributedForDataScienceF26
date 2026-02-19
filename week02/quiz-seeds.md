# Week 2 Quiz Seeds: Distributed Computing & S3 Fundamentals

## Instructions
This file contains 20 quiz questions covering the introductory distributed computing concepts and S3 setup from Week 2 lecture and lab materials.

---

## Part 1: Client-Server Architectures (Questions 1-5)

### Question 1
**Topic:** Client-only architecture
**Question:** In the cannon trajectory simulator example, where do the physics calculations happen?
**A)** On a remote AWS server
**B)** In the browser using JavaScript
**C)** In a database
**D)** On a CDN edge location
**Correct Answer:** B
**Explanation:** The simulator is a client-only implementation where all calculations happen in the browser using JavaScript. There is no server involved.

### Question 2
**Topic:** Client-server architecture trade-offs
**Question:** What is a PRIMARY advantage of using a client + API server architecture instead of client-only?
**A)** Faster response times
**B)** Works offline
**C)** Can log usage and validate inputs on the server, use many other features that are not possible in client-only
**D)** No infrastructure costs
**Correct Answer:** C
**Explanation:** Client + API server architecture allows server-side validation, usage logging, rate limiting, and protecting calculation logic, though it's slower and costs more than client-only.

### Question 3
**Topic:** Client-server architecture patterns
**Question:** Which architecture pattern would be BEST for saving user trajectory simulations so they can be accessed later from any device?
**A)** Client-only
**B)** Client + API server (no database)
**C)** Client + Server + Database
**D)** Decentralized peer-to-peer
**Correct Answer:** C
**Explanation:** Client + Server + Database architecture provides persistent storage, allowing users to save and retrieve their data from any device.

### Question 4
**Topic:** Compute-intensive applications
**Question:** When would you use a client + compute server architecture instead of client-only?
**A)** When you need simple calculations
**B)** When you need to train machine learning models or run heavy simulations
**C)** When you want to work offline
**D)** When you want to minimize costs
**Correct Answer:** B
**Explanation:** Client + compute server is used for computationally expensive tasks like ML training, video rendering, or complex simulations that would be too slow or impossible in a browser.

### Question 5
**Topic:** Client-server communication
**Question:** In a client + API server architecture, what protocol is typically used for communication?
**A)** FTP
**B)** HTTP/HTTPS
**C)** SMTP
**D)** SSH
**Correct Answer:** B
**Explanation:** HTTP/HTTPS is the standard protocol for client-server communication in web applications, used for API calls and data exchange.

---

## Part 2: Distributed System Types (Questions 6-9)

### Question 6
**Topic:** Centralized systems
**Question:** What is the main disadvantage of a centralized system architecture?
**A)** Too expensive
**B)** Single point of failure
**C)** Too complex to build
**D)** Slow consensus mechanisms
**Correct Answer:** B
**Explanation:** In a centralized system, if the central server crashes, the entire system goes down. This single point of failure is the primary weakness.

### Question 7
**Topic:** Distributed systems
**Question:** Which type of distributed system architecture does AWS primarily use?
**A)** Centralized
**B)** Distributed (coordinator + workers)
**C)** Decentralized (peer-to-peer)
**D)** Hybrid centralized-decentralized
**Correct Answer:** B
**Explanation:** AWS uses a distributed architecture with coordinators managing worker nodes, allowing for scalability and fault tolerance while maintaining coordination.

### Question 8
**Topic:** Two Generals Problem
**Question:** The Two Generals Problem illustrates which fundamental challenge in distributed systems?
**A)** High infrastructure costs
**B)** Slow network speeds
**C)** Impossibility of guaranteed coordination with unreliable communication
**D)** Difficulty of writing distributed code
**Correct Answer:** C
**Explanation:** The Two Generals Problem demonstrates that perfect synchronization is impossible in distributed systems with unreliable communication—there's no way to guarantee 100% certainty.

### Question 9
**Topic:** Decentralized systems
**Question:** Which of the following is an example of a decentralized (peer-to-peer) system?
**A)** AWS S3
**B)** Netflix
**C)** Bitcoin
**D)** Google Search
**Correct Answer:** C
**Explanation:** Bitcoin is a decentralized peer-to-peer system with no central coordinator. AWS S3, Netflix, and Google Search are all distributed systems with central coordination.

---

## Part 3: Cloud Computing Concepts (Questions 10-13)

### Question 10
**Topic:** Cloud computing definition
**Question:** According to AWS, what are the two key characteristics of cloud computing?
**A)** Fast and secure
**B)** On-demand delivery and pay-as-you-go pricing
**C)** Distributed and decentralized
**D)** Cheap and reliable
**Correct Answer:** B
**Explanation:** AWS defines cloud computing as "on-demand delivery" of IT resources with "pay-as-you-go pricing"—you get what you need when you need it, and only pay for what you use.

### Question 11
**Topic:** OpEx vs CapEx
**Question:** What is the PRIMARY tax advantage of cloud computing (OpEx) over buying servers (CapEx)?
**A)** Cloud services are tax-free
**B)** OpEx is fully deductible in the year incurred; CapEx must be depreciated over 3-7 years
**C)** CapEx provides better tax benefits
**D)** There is no tax difference
**Correct Answer:** B
**Explanation:** OpEx (cloud spending) is fully deductible in the year incurred, providing immediate tax benefits. CapEx (buying servers) must be depreciated over 3-7 years, spreading the tax benefit over time.




### Question 12
**Topic:** OpEx business advantages
**Question:** A company needs $100,000 worth of computing resources. With CapEx (buying servers), they depreciate over 5 years and deduct $20,000/year. With OpEx (cloud), they deduct the full $100,000 immediately. At a 21% corporate tax rate, what is the first-year tax savings difference between OpEx and CapEx?
**A)** $4,200 more with OpEx
**B)** $10,500 more with OpEx
**C)** $16,800 more with OpEx
**D)** $21,000 more with OpEx
**Correct Answer:** C
**Explanation:** CapEx: $20,000 × 21% = $4,200 first-year savings. OpEx: $100,000 × 21% = $21,000 first-year savings. Difference: $21,000 - $4,200 = $16,800 more with OpEx in year one.

### Question 13
**Topic:** AWS service categories
**Question:** Which AWS service category does S3 belong to?
**A)** Compute
**B)** Storage
**C)** Database
**D)** Analytics
**Correct Answer:** B
**Explanation:** S3 (Simple Storage Service) is AWS's primary object storage service, falling under the Storage category.

---

## Part 4: S3 Fundamentals (Questions 14-20)

### Question 14
**Topic:** S3 buckets
**Question:** What is TRUE about S3 bucket names?
**A)** They must be unique within your AWS account
**B)** They must be globally unique across all AWS accounts
**C)** They can contain uppercase letters
**D)** They are automatically replicated to all regions
**Correct Answer:** B
**Explanation:** S3 bucket names must be globally unique across ALL AWS accounts worldwide. They must be lowercase, and buckets are region-specific (not automatically replicated).

### Question 15
**Topic:** S3 objects
**Question:** What is the maximum size of a single object in S3?
**A)** 5 GB
**B)** 100 GB
**C)** 5 TB
**D)** Unlimited
**Correct Answer:** C
**Explanation:** S3 supports objects up to 5 TB in size. For uploads larger than 5 GB, you must use multipart upload.

### Question 16
**Topic:** S3 RESTful API
**Question:** Which HTTP method is used to upload a file to S3?
**A)** GET
**B)** POST
**C)** PUT
**D)** UPLOAD
**Correct Answer:** C
**Explanation:** S3 uses the PUT HTTP method to upload/create objects. GET retrieves objects, DELETE removes them, and HEAD gets metadata.

### Question 17
**Topic:** S3 capabilities
**Question:** Which of the following can S3 do?
**A)** Host a static website with HTML, CSS, and JavaScript
**B)** Run server-side Python code
**C)** Execute SQL queries directly on stored data
**D)** Act as a relational database
**Correct Answer:** A
**Explanation:** S3 can host static websites (HTML, CSS, JS) but cannot run server-side code, execute SQL queries directly, or function as a database. It's object storage, not a compute or database service.

### Question 18
**Topic:** S3 storage classes
**Question:** Which S3 storage class is the CHEAPEST for long-term archival storage that is rarely accessed?
**A)** S3 Standard
**B)** S3 Intelligent-Tiering
**C)** S3 Glacier Flexible Retrieval
**D)** S3 Glacier Deep Archive
**Correct Answer:** D
**Explanation:** S3 Glacier Deep Archive is the cheapest storage class (~$0.00099/GB/month) designed for data accessed less than once per year, with retrieval times of 12-48 hours.

### Question 19
**Topic:** S3 security
**Question:** What are the TWO primary ways to control access to S3 objects?
**A)** Firewalls and VPNs
**B)** Bucket policies and IAM policies
**C)** SSH keys and passwords
**D)** API keys and OAuth tokens
**Correct Answer:** B
**Explanation:** S3 access is controlled through bucket policies (attached to buckets) and IAM policies (attached to users/roles). These define who can access what resources.

### Question 20
**Topic:** S3 pricing
**Question:** What are the THREE main components of S3 pricing?
**A)** Storage, requests, and data transfer
**B)** CPU, memory, and storage
**C)** Bandwidth, compute, and database
**D)** Servers, networking, and security
**Correct Answer:** A
**Explanation:** S3 pricing has three main components: storage (per GB/month), requests (PUT, GET, DELETE operations), and data transfer (especially data OUT of AWS). There are no compute or server costs.

 
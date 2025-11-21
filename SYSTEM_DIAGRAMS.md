# BRPS System Diagrams

## Table of Contents
1. [System Sequence Diagrams](#system-sequence-diagrams)
2. [Entity Relationship Diagram](#entity-relationship-diagram)
3. [Component Architecture Diagram](#component-architecture-diagram)

---

## System Sequence Diagrams

### 1. Application Startup Sequence

```mermaid
sequenceDiagram
    participant User
    participant MainWindow
    participant MainViewModel
    participant JobRepository
    participant LogRepository
    participant Database
    participant AutoRunTimer

    User->>MainWindow: Launch Application
    MainWindow->>MainViewModel: Window_Loaded()
    MainViewModel->>MainViewModel: initLog()
    MainViewModel->>JobRepository: GetBusinessDate("06:00")
    JobRepository->>Database: Query Business Date
    Database-->>JobRepository: Return Date
    JobRepository-->>MainViewModel: Business Date
    
    MainViewModel->>LogRepository: GetAppLogsByNum(100, Date)
    LogRepository->>Database: Query Recent Logs
    Database-->>LogRepository: Return Logs
    LogRepository-->>MainViewModel: Log Collection
    
    MainViewModel->>JobRepository: GetAllScheduledJobs()
    JobRepository->>Database: Query Scheduled Jobs
    Database-->>JobRepository: Return Jobs
    JobRepository-->>MainViewModel: Job Collection
    
    MainViewModel->>MainViewModel: Check AutoRunSetting
    alt AutoRun Enabled
        MainViewModel->>MainViewModel: Run_AutoMode()
        MainViewModel->>AutoRunTimer: Start Timer (30s interval)
        AutoRunTimer-->>MainViewModel: Timer Started
    end
    
    MainViewModel-->>MainWindow: UI Ready
    MainWindow-->>User: Display Application
```

### 2. Auto-Run Mode Execution Sequence

```mermaid
sequenceDiagram
    participant Timer
    participant MainViewModel
    participant ScheduleJobService
    participant Repository
    participant ExternalSystem as External System (TE/GLOSS)
    participant Database

    Timer->>MainViewModel: OnAutoRunTimer_Tick()
    MainViewModel->>MainViewModel: Check Current Time
    
    alt Time Window Match (e.g., 16:00-24:00)
        loop For Each Scheduled Job
            MainViewModel->>MainViewModel: Check Job Status
            alt Job Not Done & In Time Window
                MainViewModel->>MainViewModel: AddAppLog("Starting job...")
                MainViewModel->>ScheduleJobService: Execute(jobModel, businessDate, true, logger)
                
                ScheduleJobService->>ScheduleJobService: Validate Time Window
                ScheduleJobService->>ScheduleJobService: Build Parameters
                ScheduleJobService->>ScheduleJobService: Invoke Job Method (Reflection)
                
                alt Load Static Data Job
                    ScheduleJobService->>Repository: GetDataFromTE()
                    Repository->>ExternalSystem: Query Data
                    ExternalSystem-->>Repository: Return Data
                    Repository->>Database: Insert/Update Data
                    Database-->>Repository: Success
                    Repository-->>ScheduleJobService: Record Count
                else Load Trade Job
                    ScheduleJobService->>Repository: GetTradeListFromTE(date)
                    Repository->>ExternalSystem: Query Trades
                    ExternalSystem-->>Repository: Return Trades
                    Repository->>Database: Insert Trades
                    Database-->>Repository: Success
                    Repository-->>ScheduleJobService: Record Count
                end
                
                ScheduleJobService->>ScheduleJobService: Update Job Status
                ScheduleJobService-->>MainViewModel: Execution Result
                
                MainViewModel->>Repository: EditScheduledJob(job)
                Repository->>Database: Update Job Status
                Database-->>Repository: Success
                
                MainViewModel->>MainViewModel: AddAppLog("Job completed")
                
                alt Job Completed Successfully & Exit Condition
                    MainViewModel->>MainViewModel: Environment.Exit(0)
                end
            end
        end
    end
```

### 3. Manual Job Execution Sequence

```mermaid
sequenceDiagram
    participant User
    participant MainWindow
    participant MainViewModel
    participant ScheduleJobService
    participant Repository
    participant ExternalSystem as External System
    participant Database

    User->>MainWindow: Select Jobs
    User->>MainWindow: Click Execute Button
    MainWindow->>MainViewModel: ExecuteCommand
    
    loop For Each Selected Job
        MainViewModel->>MainViewModel: Check Job Selection
        alt Job Selected
            MainViewModel->>MainWindow: Set Cursor to Wait
            MainViewModel->>MainViewModel: AddAppLog("INFO", "Starting...")
            
            MainViewModel->>ScheduleJobService: Execute(jobModel, businessDate, false, logger)
            
            ScheduleJobService->>ScheduleJobService: Check Time Window
            alt Not In Time Window
                ScheduleJobService->>User: Show Confirmation Dialog
                User-->>ScheduleJobService: Confirm/Cancel
            end
            
            alt User Confirmed or In Time Window
                ScheduleJobService->>ScheduleJobService: Build Parameters
                ScheduleJobService->>ScheduleJobService: Invoke Method via Reflection
                
                ScheduleJobService->>Repository: Execute Data Operation
                Repository->>ExternalSystem: Fetch Data
                ExternalSystem-->>Repository: Return Data
                Repository->>Database: Persist Data
                Database-->>Repository: Success/Failure
                Repository-->>ScheduleJobService: Result Count
                
                ScheduleJobService->>ScheduleJobService: Update Job Status
                ScheduleJobService-->>MainViewModel: Execution Result
            end
            
            MainViewModel->>Repository: EditScheduledJob(job)
            Repository->>Database: Update Job Record
            Database-->>Repository: Success
            
            MainViewModel->>MainViewModel: AddAppLog(status, message)
            MainViewModel->>MainWindow: Set Cursor to Arrow
        end
    end
    
    MainWindow-->>User: Display Results
```

### 4. ETL Job Execution Detail (Load Trades Example)

```mermaid
sequenceDiagram
    participant Service as ScheduleJobService
    participant TradeRepo as TradeRepository
    participant TE as Trading Engine (TE)
    participant GenericDAO
    participant Database
    participant Logger

    Service->>Logger: Info("+LoadTrades_EOD_CAGS()")
    Service->>Service: Validate lastBusinessDate
    
    Service->>TradeRepo: new TradeRepository()
    Service->>Logger: Info("GetTradeListFromTE_CAGS")
    
    TradeRepo->>GenericDAO: ExecuteStoredProcedure()
    GenericDAO->>TE: Call SP_GET_TRADES_CAGS(date)
    Note over GenericDAO,TE: Timeout: 3600 seconds
    TE-->>GenericDAO: Return Trade Records
    GenericDAO-->>TradeRepo: DataTable
    
    TradeRepo->>TradeRepo: Map DataTable to Trade Objects
    TradeRepo-->>Service: List<Trade>
    
    Service->>Logger: Info("InsertTrades_CAGS")
    Service->>TradeRepo: InsertTrades_CAGS(tradeList, date)
    
    TradeRepo->>Database: Begin Transaction
    loop For Each Trade
        TradeRepo->>Database: Check if Trade Exists
        alt Trade Exists
            TradeRepo->>Database: Update Trade
        else New Trade
            TradeRepo->>Database: Insert Trade
        end
    end
    TradeRepo->>Database: Commit Transaction
    Database-->>TradeRepo: Success
    
    TradeRepo-->>Service: Success
    Service->>Logger: Info("-LoadTrades_EOD_CAGS()")
    Service-->>Service: Return tradeList.Count
```

### 5. Job Management Sequence (Add/Edit/Delete)

```mermaid
sequenceDiagram
    participant User
    participant MainWindow
    participant MainViewModel
    participant EditWindow as EditScheduledJobWindow
    participant EditVM as EditScheduledJobVM
    participant JobRepository
    participant Database

    alt Add New Job
        User->>MainWindow: Click Add Job
        MainWindow->>MainViewModel: AddJobCommand
        MainViewModel->>EditWindow: new EditScheduledJobWindow()
        MainViewModel->>EditVM: Send Message (null job)
        EditWindow-->>User: Display Empty Form
        
        User->>EditWindow: Enter Job Details
        User->>EditWindow: Click Save
        EditWindow->>EditVM: SaveCommand
        EditVM->>JobRepository: AddScheduledJob(job)
        JobRepository->>Database: INSERT INTO ScheduledJob
        Database-->>JobRepository: Success
        JobRepository-->>EditVM: Success
        EditVM->>EditWindow: Close()
        EditWindow-->>MainViewModel: Dialog Result
        MainViewModel->>MainViewModel: LoadScheduledJobs()
    end
    
    alt Edit Existing Job
        User->>MainWindow: Select Job & Click Edit
        MainWindow->>MainViewModel: EditJobCommand
        MainViewModel->>EditWindow: new EditScheduledJobWindow()
        MainViewModel->>EditVM: Send Message (selected job)
        EditWindow-->>User: Display Job Details
        
        User->>EditWindow: Modify Job Details
        User->>EditWindow: Click Save
        EditWindow->>EditVM: SaveCommand
        EditVM->>JobRepository: EditScheduledJob(job)
        JobRepository->>Database: UPDATE ScheduledJob
        Database-->>JobRepository: Success
        JobRepository-->>EditVM: Success
        EditVM->>EditWindow: Close()
        EditWindow-->>MainViewModel: Dialog Result
        MainViewModel->>MainViewModel: LoadScheduledJobs()
    end
    
    alt Delete Job
        User->>MainWindow: Select Job & Click Delete
        MainWindow->>MainViewModel: DeleteJobCommand
        MainViewModel->>User: Show Confirmation Dialog
        User-->>MainViewModel: Confirm
        MainViewModel->>JobRepository: DeleteScheduledJob(job)
        JobRepository->>Database: DELETE FROM ScheduledJob
        Database-->>JobRepository: Success
        JobRepository-->>MainViewModel: Success
        MainViewModel->>MainViewModel: LoadScheduledJobs()
        MainViewModel->>User: Show Success Message
    end
```

### 6. Time-Based Job Scheduling Flow

```mermaid
sequenceDiagram
    participant Timer
    participant MainViewModel
    participant System

    Note over Timer,System: Application Running in Auto Mode
    
    loop Every 30 Seconds
        Timer->>MainViewModel: Timer Tick Event
        MainViewModel->>MainViewModel: Get Current Time
        
        alt 16:00 - 24:00 (Static Data Window)
            MainViewModel->>MainViewModel: OnAutoRunTimer_Tick()
            Note over MainViewModel: Execute Static Data Jobs:<br/>- Load Instruments<br/>- Load Instruments Price<br/>- Load Salespersons<br/>- Load Clients
            MainViewModel->>MainViewModel: OnExecuteCommand(date, true)
        else 00:00 - 04:00 (EOD Trade Window)
            MainViewModel->>MainViewModel: OnAutoRunTimer_Tick1()
            Note over MainViewModel: Execute EOD Trade Jobs:<br/>- Load Stock/Cash Balance<br/>- Load Trade Narrative<br/>- Load Amended/Cancelled Trades<br/>- Load BAGS/CAGS/CCAC/SCCF/FRES/FREC
            MainViewModel->>MainViewModel: OnExecuteCommand1(date, true)
        else 05:00 - 11:00 (US Trade Window)
            MainViewModel->>MainViewModel: OnAutoRunTimer_Tick2()
            Note over MainViewModel: Execute US Trade Jobs:<br/>- Load USTRADE
            MainViewModel->>MainViewModel: OnExecuteCommand2(date, true)
        else 12:00 - 23:00 (Central Dealer Window)
            MainViewModel->>MainViewModel: OnAutoRunTimer_Tick4()
            Note over MainViewModel: Execute Central Dealer Jobs:<br/>- Load CDEALER
            MainViewModel->>MainViewModel: OnExecuteCommand4(date, true)
        end
        
        alt Critical Job Completed Successfully
            MainViewModel->>System: Environment.Exit(0)
            Note over MainViewModel,System: Application terminates after<br/>successful completion of<br/>critical jobs
        end
    end
```

---

## Entity Relationship Diagram

```mermaid
erDiagram
    Party ||--o| Client : "has"
    Party ||--o| Salesperson : "has"
    Party ||--o{ Address : "has multiple"
    
    Client ||--o| ClientAccount : "belongs to"
    ClientAccount ||--o{ Client : "contains"
    
    Trade ||--o| TradeCharge : "has"
    Trade }o--|| Client : "placed by"
    Trade }o--|| Instrument : "trades"
    
    Instrument ||--o| Warrant : "may be"
    Instrument }o--|| Market : "listed on"
    
    ScheduledJob ||--o{ ScheduledJobParam : "has parameters"
    
    Client ||--o{ Client_CashBalance : "has"
    Client ||--o{ Client_StockBalance : "has"
    
    Trade ||--o| Trade_Status : "has status"
    Trade ||--o| Trade_NarrativeInfo : "has narrative"
    Trade ||--o| Trade_Cancelled : "may be cancelled"
    Trade ||--o| TradeCharge_AmendedComm : "may have amendment"
    
    Party {
        string Party_Ref PK
        string Company
        string Party_Category
        int Version_No
        datetime Version_Date
    }
    
    Client {
        string Client_Number PK
        string Party_Ref FK
        string Client_Account_Number FK
        string Client_Title
        string Short_Name
        string Long_Name
        string Marital_Status
        datetime Birth_Incorp_Date
        string SG_PR
        string Identification_Type
        string Unique_ID
        string Issuing_Country
        string Ownership_Category_Code
        string Principal_Business
        string Residence_Code
        string Occupation
        string Employment_Status
        string Client_Type
        string Client_Status
        int Version_No
        datetime Version_Date
        string Status
        string Country
    }
    
    ClientAccount {
        string Client_Account_Number PK
        string Position_Account_Number
        string Position_Account_Category
        string Account_Status
        string Position_Account_Type
        string Remisier_Code
        datetime Created_Date
        datetime Activated_Date
        datetime Closed_Date
        decimal Buy_Limit
        decimal Sell_Limit
        decimal Margin_Credit_Limit
        string Payment_Mode
        string EPS_Link_Status
        string EPS_Bank_No
        string EPS_Bank_Acct
        string Status_Narrative
    }
    
    Salesperson {
        string Salesperson_Ref PK
        string Party_Ref FK
        string Short_Name
        string Long_Name
        string Country
        string Location
        string Title
        string Active
        string Dept_Code
        string Sales_Group
        string MAS_Rep_No
        string SGX_TR_Code
        datetime Joining_Date
        datetime Departure_Date
        int Version_No
        datetime Version_Date
    }
    
    Trade {
        datetime Business_Date PK
        long Trade_No PK
        string Trade_Ref
        string Client_Number FK
        string Trade_Type
        string Sub_Trade_Type
        string Operation_Type
        string Market
        datetime Trade_Date
        datetime Value_Date
        string Instrument FK
        long Qty
        long Open_Qty
        decimal Price
        string Trade_Curr
        decimal Trade_Principal
        string Settle_Curr
        decimal Settle_Amount
        decimal Open_Amount
        decimal Exch_Rate
        string Trade_Status
        string Settle_Status
        string Primary_Book
        string SGX_Trade_No
        string Execution_SalesPerson
        int Trade_Version
        datetime Created_Date
        datetime Amend_Date
    }
    
    TradeCharge {
        datetime Business_Date PK
        long Trade_No PK
        string Curr
        decimal Commission
        decimal Commission_GST
        decimal Clearing_Fee
        decimal Clearing_Fee_GST
        decimal Trading_Fee
        decimal Trading_Fee_GST
        decimal Settlement_Fee
        decimal Stamp_Duty_Fee
        decimal Transaction_Levy_Fee
        decimal Commission_SGD
        decimal Clearing_Fee_SGD
        decimal Trading_Fee_SGD
    }
    
    Instrument {
        string Instr_Ref PK
        string Instr_Group
        string Long_Name
        string Short_Name
        string Main_Market FK
        string Denom_Ccy
        int Denom_Qty
        string Book
        int Price_Decimal_Place
        decimal Price_Divisor
        decimal Price_Multiplier
        string Active_Ind
        datetime Listing_Date
        datetime Delisting_Date
        string ISIN_Code
        string Security_Type
        string Country
        decimal Close_Price
        int Version_No
        datetime Version_Date
    }
    
    ScheduledJob {
        int Job_Id PK
        string Description
        int Depend_Job_Id
        string Occurence
        int Interval
        string From_Time
        string End_Time
        string Process
        int Status
        datetime Business_Date
        datetime Last_Execution_Time
        string Message
    }
    
    ScheduledJobParam {
        int Param_Id PK
        int Job_Id FK
        int Param_Order
        string Param_Name
        string Param_Type
        string Param_Value
    }
    
    Address {
        string Party_Ref FK
        string Address_Type
        string Address_Line_1
        string Address_Line_2
        string City
        string State
        string Postal_Code
        string Country
    }
    
    Market {
        string Market_Code PK
        string Market_Name
        string Country
        string Currency
    }
    
    Warrant {
        string Instr_Ref PK
        string Underlying_Instr
        datetime Expiry_Date
        decimal Strike_Price
        string Warrant_Type
    }
    
    Client_CashBalance {
        string Client_Number PK
        datetime Business_Date PK
        string Currency PK
        decimal Balance_Amount
        decimal Available_Amount
        decimal Blocked_Amount
    }
    
    Client_StockBalance {
        string Client_Number PK
        datetime Business_Date PK
        string Instrument PK
        long Balance_Qty
        long Available_Qty
        long Blocked_Qty
    }
    
    Trade_Status {
        datetime Business_Date PK
        long Trade_No PK
        string Status_Code
        datetime Status_Date
        string Status_Reason
    }
    
    Trade_NarrativeInfo {
        datetime Business_Date PK
        long Trade_No PK
        string Narrative_Type
        string Narrative_Text
    }
    
    Trade_Cancelled {
        datetime Business_Date PK
        long Trade_No PK
        datetime Cancelled_Date
        string Cancelled_Reason
        string Cancelled_By
    }
    
    TradeCharge_AmendedComm {
        datetime Business_Date PK
        long Trade_No PK
        datetime Amendment_Date
        decimal Original_Commission
        decimal Amended_Commission
        string Amendment_Reason
    }
    
    AppLog {
        int Log_Id PK
        datetime Log_Time
        string Job_Desc
        string Status
        string Message
    }
    
    ExchangeRate {
        string From_Curr PK
        string To_Curr PK
        datetime Rate_Date PK
        decimal Exchange_Rate
        string Mult_Div_Ind
    }
    
    Holiday {
        string Market PK
        datetime Holiday_Date PK
        string Holiday_Name
        string Holiday_Type
    }
    
    Instrument_Price {
        string Instr_Ref PK
        datetime Price_Date PK
        decimal Open_Price
        decimal High_Price
        decimal Low_Price
        decimal Close_Price
        long Volume
    }
```

---

## Component Architecture Diagram

```mermaid
graph TB
    subgraph "Presentation Layer - WPF"
        UI[MainWindow.xaml]
        EditUI[EditScheduledJobWindow.xaml]
        Controls[Custom Controls]
    end
    
    subgraph "ViewModel Layer - MVVM"
        MainVM[MainViewModel]
        EditVM[EditScheduledJobVM]
        VMLocator[ViewModelLocator]
    end
    
    subgraph "Business Logic Layer"
        JobService[ScheduleJobService]
        DataMgr[DataManagerBase]
        AutoTimer[Auto-Run Timer]
    end
    
    subgraph "Data Access Layer"
        subgraph "Repositories"
            JobRepo[ScheduledJobRepository]
            TradeRepo[TradeRepository]
            PartyRepo[PartyRepository]
            InstrRepo[InstrumentRepository]
            LogRepo[AppLogRepository]
            BalanceRepo[ClientBalanceRepository]
        end
        
        subgraph "DAO"
            GenericDAO[GenericDAO]
            ConnProvider[ConnectionSettingsProvider]
        end
    end
    
    subgraph "Data Layer"
        EF[Entity Framework Context]
        Models[Entity Models]
    end
    
    subgraph "External Systems"
        TE[(Trading Engine - TE)]
        GLOSS[(GLOSS System)]
    end
    
    subgraph "Database"
        BRPSDB[(BRPS SQL Server)]
    end
    
    subgraph "Logging"
        KGILog[KGILogger]
        LogFile[Log Files]
    end
    
    UI --> MainVM
    EditUI --> EditVM
    Controls --> MainVM
    
    MainVM --> VMLocator
    EditVM --> VMLocator
    
    MainVM --> JobService
    MainVM --> JobRepo
    MainVM --> LogRepo
    MainVM --> AutoTimer
    
    AutoTimer --> MainVM
    
    JobService --> TradeRepo
    JobService --> PartyRepo
    JobService --> InstrRepo
    JobService --> BalanceRepo
    JobService --> KGILog
    
    JobRepo --> GenericDAO
    TradeRepo --> GenericDAO
    PartyRepo --> GenericDAO
    InstrRepo --> GenericDAO
    LogRepo --> GenericDAO
    BalanceRepo --> GenericDAO
    
    GenericDAO --> ConnProvider
    GenericDAO --> EF
    
    EF --> Models
    EF --> BRPSDB
    
    TradeRepo -.->|Query Trades| TE
    PartyRepo -.->|Query Clients/Sales| GLOSS
    InstrRepo -.->|Query Instruments| TE
    BalanceRepo -.->|Query Balances| TE
    
    KGILog --> LogFile
    LogRepo --> BRPSDB
    
    style UI fill:#e1f5ff
    style MainVM fill:#fff4e1
    style JobService fill:#ffe1f5
    style GenericDAO fill:#e1ffe1
    style BRPSDB fill:#f5e1e1
    style TE fill:#f5e1e1
    style GLOSS fill:#f5e1e1
```

### Component Responsibilities

**Presentation Layer:**
- MainWindow: Primary UI for job management and monitoring
- EditScheduledJobWindow: Job configuration interface
- Custom Controls: Reusable UI components (ScheduledJobView, ScheduledJobExpander)

**ViewModel Layer:**
- MainViewModel: Orchestrates job execution, logging, and UI state
- EditScheduledJobVM: Manages job editing operations
- ViewModelLocator: Dependency injection container

**Business Logic Layer:**
- ScheduleJobService: Core ETL execution engine with reflection-based method invocation
- DataManagerBase: Base class for data management operations
- Auto-Run Timer: Time-based job scheduling (30-second intervals)

**Data Access Layer:**
- Repositories: Domain-specific data access (Repository Pattern)
- GenericDAO: Generic database operations with timeout handling
- ConnectionSettingsProvider: Database connection management

**Data Layer:**
- Entity Framework Context: ORM for database operations
- Entity Models: Auto-generated from database schema

**External Systems:**
- Trading Engine (TE): Source for trades, instruments, prices
- GLOSS: Source for client and salesperson data

---

## Data Flow Diagram

```mermaid
flowchart LR
    subgraph External["External Systems"]
        TE[Trading Engine]
        GLOSS[GLOSS System]
    end
    
    subgraph ETL["ETL Process"]
        Extract[Extract Data]
        Transform[Transform & Validate]
        Load[Load to Database]
    end
    
    subgraph BRPS["BRPS Application"]
        Scheduler[Job Scheduler]
        Service[Job Service]
        Repo[Repositories]
    end
    
    subgraph Storage["Data Storage"]
        DB[(BRPS Database)]
        Logs[Application Logs]
    end
    
    subgraph UI["User Interface"]
        Monitor[Job Monitor]
        Config[Job Configuration]
    end
    
    TE -->|Trades, Instruments, Prices| Extract
    GLOSS -->|Clients, Salespersons| Extract
    
    Extract --> Transform
    Transform --> Load
    Load --> DB
    
    Scheduler -->|Trigger| Service
    Service -->|Execute| Repo
    Repo -->|Query| TE
    Repo -->|Query| GLOSS
    Repo -->|Persist| DB
    
    Service -->|Write| Logs
    DB -->|Read| Monitor
    Logs -->|Display| Monitor
    
    Config -->|Update| DB
    Monitor -->|Manual Trigger| Scheduler
    
    style TE fill:#ffcccc
    style GLOSS fill:#ffcccc
    style DB fill:#ccffcc
    style Scheduler fill:#ccccff
```

---

*Diagrams generated on November 21, 2025*

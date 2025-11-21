# BRPS - Brokerage Reporting and Processing System

## Project Overview

**BRPS** (Brokerage Reporting and Processing System) is a mission-critical Windows Presentation Foundation (WPF) desktop application designed for automated ETL (Extract, Transform, Load) operations in a brokerage/trading environment. The system serves as the primary data synchronization bridge between external trading systems and internal reporting databases, processing thousands of trades, client records, and financial instruments daily.

### Business Context

BRPS operates within a multi-market brokerage environment handling:
- **Singapore Exchange (SGX)** trades and settlements
- **US market** trades (USTRADE)
- **Central dealer** operations (CDEALER)
- Multiple settlement systems: BAGS, CAGS, CCAC, SCCF, FRES, FREC
- Client account management across multiple jurisdictions
- Real-time instrument pricing and market data

The application runs 24/7 in production environments across multiple regions (Singapore, Hong Kong, MIT) with different time-zone-based execution windows to accommodate global market hours.

### Technology Stack

**Core Framework:**
- .NET Framework 4.8
- C# 7.0
- WPF (Windows Presentation Foundation) for UI
- MVVM Light Framework 5.2.0 for MVVM pattern implementation

**Data Access:**
- Entity Framework 6.1.3 (Code-First approach with EDMX)
- Entity Framework Extended 6.1.0.168 for bulk operations
- ADO.NET for direct database access
- Sybase ASE Client for Trading Engine connectivity

**Database Systems:**
- SQL Server (BRPS local database)
- Sybase ASE (Trading Engine - TE)
- GLOSS (General Ledger and Operations Support System)

**Logging & Monitoring:**
- Log4Net for file-based logging
- Custom KGILogger wrapper
- Application log database table for UI display

**Deployment:**
- ClickOnce deployment technology
- Network share distribution
- Auto-update capability

**Current Version:** 1.0.0.13 (Last updated: October 6, 2017)

### System Characteristics

**Performance:**
- Processes 10,000+ trade records per batch
- Batch size: 2,000 records per database transaction
- Query timeout: 3,600 seconds (1 hour) for large data extracts
- 30-second polling interval for auto-run mode

**Reliability:**
- Transaction-based data persistence
- Automatic retry logic (implicit in timer-based execution)
- Validation error handling with detailed logging
- Connection pooling for database efficiency

**Scalability:**
- Multi-environment support (PP, GLS, MIT, BDM)
- Configurable connection strings per environment
- Parameterized job execution
- Reflection-based method invocation for extensibility

---

## System Purpose

BRPS serves as a comprehensive scheduled job management and execution system that fulfills critical business functions:

### 1. Master Data Synchronization

**Instruments Management:**
- Extracts instrument master data from Trading Engine (TE)
- Synchronizes 5,000+ financial instruments including stocks, bonds, warrants, derivatives
- Updates instrument attributes: ISIN codes, Bloomberg codes, RIC codes, security types
- Maintains pricing information with decimal precision handling
- Tracks listing/delisting dates and active status
- Supports multiple markets: SGX, KLSE, US exchanges

**Client Data Management:**
- Synchronizes client master data from GLOSS system
- Manages 10,000+ client records with full KYC information
- Tracks client account status, risk profiles, trading limits
- Maintains client-account relationships and hierarchies
- Updates client identification details, addresses, and contact information
- Supports multiple client types: individual, corporate, trust accounts

**Salesperson/Remisier Data:**
- Synchronizes sales representative information
- Maintains commission structures and reporting hierarchies
- Tracks MAS registration numbers and SGX trader codes
- Manages active/inactive status and departure dates

**Pricing Data:**
- Downloads end-of-day instrument prices
- Processes FX rates for multi-currency settlements
- Updates close prices for portfolio valuation
- Maintains historical price data for reporting

### 2. Trade Processing

**Multi-Platform Trade Downloads:**
- **BAGS (Broker Assisted General Settlement)**: Manual trade settlements
- **CAGS (Central Assisted General Settlement)**: Automated SGX settlements
- **CCAC (Central Counterparty Clearing)**: Cleared trades through CCP
- **SCCF (Securities Clearing and Settlement)**: Standard clearing trades
- **FRES (Foreign Exchange Settlement)**: FX trade settlements
- **FREC (Foreign Exchange Clearing)**: FX cleared trades (Open & Settled)
- **USTRADE**: US market trades with T+2 settlement
- **CDEALER**: Central dealer proprietary trades

**Trade Data Captured:**
- Trade identification: Trade number, reference, version
- Parties: Client number, salesperson, counterparty
- Instrument details: Symbol, ISIN, quantity, price
- Financial details: Principal amount, settlement amount, exchange rates
- Settlement: Value date, settlement currency, payment mode
- Charges: Commission, GST, clearing fees, stamp duty, transaction levy
- Status: Trade status, settlement status, payment status
- Audit: Created date, amended date, version history

### 3. Balance Management

**Client Cash Balances:**
- Multi-currency cash position tracking
- Available vs. blocked amounts
- Real-time balance updates from TE
- Support for SGD, USD, HKD, and other currencies

**Client Stock Balances:**
- Security position tracking by instrument
- Available vs. blocked quantities
- Corporate action adjustments
- Custodian account reconciliation

### 4. Trade Lifecycle Management

**Trade Amendments:**
- Tracks commission amendments (past 20 days)
- Maintains amendment history and reasons
- Compares original vs. amended values
- Audit trail for compliance

**Trade Cancellations:**
- Records cancelled trades with reasons
- Maintains cancellation audit trail
- Tracks cancelled by user and timestamp
- Prevents duplicate processing

**Trade Status Tracking:**
- Monitors trade progression through lifecycle
- Tracks settlement status changes
- Records status change timestamps
- Maintains 1-month rolling window for performance

**Trade Narratives:**
- Stores trade-specific notes and comments
- Supports multiple narrative types
- Maintains narrative history
- Used for exception handling and client communication

### 5. Automated Scheduling

**Time-Window Based Execution:**
- **16:00-24:00**: Static data refresh (instruments, clients, salespersons, prices)
- **00:00-04:00**: EOD trade downloads and balance updates
- **05:00-11:00**: US market trade processing
- **12:00-23:00**: Central dealer trade processing

**Scheduling Features:**
- 30-second polling interval for job checks
- Configurable time windows per job
- Job dependency management
- Business date calculation with cutoff times
- Automatic application termination after critical job completion

### 6. Operational Monitoring

**Real-Time Logging:**
- Application log viewer with last 100 entries
- Status indicators: INFO, ERR
- Job execution timestamps
- Record counts and error messages
- Auto-scroll to latest entries

**Job Status Tracking:**
- Visual status indicators (Success/Error/None)
- Last execution time display
- Business date tracking
- Job message display
- Execution history

### 7. Data Quality & Compliance

**Validation:**
- Entity Framework validation rules
- Business rule validation in repositories
- Data type and constraint checking
- Referential integrity enforcement

**Audit Trail:**
- Complete job execution history
- Error logging with stack traces
- Version tracking on all entities
- Timestamp tracking for all operations

**Compliance:**
- MAS (Monetary Authority of Singapore) reporting support
- SGX regulatory data capture
- Client identification and KYC data maintenance
- Trade reporting requirements

---

## Architecture

### Architectural Overview

BRPS follows a layered architecture pattern with clear separation of concerns:

```
┌─────────────────────────────────────────────────────────────┐
│                    Presentation Layer                        │
│  (WPF Views, XAML, Custom Controls, Data Binding)          │
└─────────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────────┐
│                    ViewModel Layer                           │
│  (MVVM Light, Commands, Observable Collections, Messaging)  │
└─────────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────────┐
│                  Business Logic Layer                        │
│  (Job Scheduling, ETL Processing, Business Rules)           │
└─────────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────────┐
│                  Data Access Layer                           │
│  (Repositories, Entity Framework, Generic DAO)              │
└─────────────────────────────────────────────────────────────┘
                            ↕
┌─────────────────────────────────────────────────────────────┐
│              External Systems & Databases                    │
│  (SQL Server, Sybase ASE, Trading Engine, GLOSS)           │
└─────────────────────────────────────────────────────────────┘
```

### Project Structure

```
BRPS/
├── DAO/                                    # Data Access Objects Layer
│   ├── GenericDAO.cs                      # Generic database operations with timeout handling
│   │   • ExecuteQuery() - DataSet retrieval with 3600s timeout
│   │   • ExecuteNonQuery() - Command execution
│   │   • ExecuteReader() - Forward-only data reading
│   │   • Connection management and disposal
│   ├── ConnectionSettingsProvider.cs      # Connection string provider
│   └── IConnectionSettingsProvider.cs     # Connection provider interface
│
├── Managers/                               # Business Logic Layer
│   ├── DataManagerBase.cs                 # Base class for data managers
│   └── ScheduleJobService.cs              # Core ETL job execution engine
│       • Execute() - Main job execution with reflection
│       • LoadInstruments() - Instrument synchronization
│       • LoadInstruments_Price() - Price data download
│       • LoadSalespersons() - Salesperson sync
│       • LoadClients() - Client data sync
│       • LoadTrades_EOD_CAGS/BAGS/CCAC/SCCF/FRES/FREC - Trade downloads
│       • LoadTrades_EOD_USTRADE - US trade processing
│       • LoadTrades_EOD_CDEALER - Central dealer trades
│       • LoadStockBalance() - Stock position sync
│       • LoadCashBalance() - Cash position sync
│       • LoadTradeNarrativeInfo() - Trade notes
│       • LoadAmendedTrade() - Amendment tracking
│       • LoadCancelledTrade() - Cancellation tracking
│
├── Model/                                  # Entity Framework Models
│   ├── BRPSModel.edmx                     # Entity Data Model (EDMX)
│   ├── BRPSModel.Context.cs               # DbContext generated code
│   ├── BRPSModel.Designer.cs              # Designer generated code
│   │
│   ├── Core Entities/
│   │   ├── ScheduledJob.cs                # Job configuration (Job_Id, Description, Process, Time Windows)
│   │   ├── ScheduledJobParam.cs           # Job parameters (Param_Name, Param_Value, Param_Type)
│   │   ├── Trade.cs                       # Trade entity (60+ fields)
│   │   ├── TradeCharge.cs                 # Trade fees and commissions
│   │   ├── Client.cs                      # Client master data (40+ fields)
│   │   ├── ClientAccount.cs               # Account details (60+ fields)
│   │   ├── Instrument.cs                  # Financial instruments (45+ fields)
│   │   ├── Party.cs                       # Party master (Client/Salesperson base)
│   │   └── Salesperson.cs                 # Salesperson/Remisier data
│   │
│   ├── Balance Entities/
│   │   ├── Client_CashBalance.cs          # Multi-currency cash positions
│   │   └── Client_StockBalance.cs         # Security positions
│   │
│   ├── Trade Lifecycle/
│   │   ├── Trade_Status.cs                # Trade status tracking
│   │   ├── Trade_NarrativeInfo.cs         # Trade notes and comments
│   │   ├── Trade_Cancelled.cs             # Cancelled trade records
│   │   └── TradeCharge_AmendedComm.cs     # Commission amendments
│   │
│   ├── Reference Data/
│   │   ├── Market.cs                      # Market master data
│   │   ├── Instrument_Price.cs            # Historical pricing
│   │   ├── ExchangeRate.cs                # FX rates
│   │   ├── Holiday.cs                     # Market holidays
│   │   ├── Warrant.cs                     # Warrant-specific data
│   │   ├── Transaction_Type.cs            # Transaction type codes
│   │   ├── Trans_Code_Desc.cs             # Transaction descriptions
│   │   └── Code_Mapping.cs                # Code mapping tables
│   │
│   ├── Supporting/
│   │   ├── Address.cs                     # Party addresses
│   │   ├── AppLog.cs                      # Application logging
│   │   └── sysdiagram.cs                  # Database diagrams
│   │
│   └── Extensions/
│       └── ScheduledJobEx.cs              # Extended job model with UI properties
│
├── Repositories/                           # Repository Pattern Implementation
│   ├── RepositoryBase.cs                  # Base repository with common operations
│   │   • InsertEntities<T>() - Batch insert (2000 records/batch)
│   │   • ExecuteNonQuery() - Command execution
│   │   • ExecuteQuery() - Query execution
│   │   • SaveChanges() - EF save with validation error handling
│   │
│   ├── ScheduledJobRepository.cs          # Job CRUD operations
│   │   • GetAllScheduledJobs() - Load all jobs with parameters
│   │   • GetScheduledJobByJobId() - Single job retrieval
│   │   • InsertScheduledJob() - Create new job
│   │   • EditScheduledJob() - Update job and parameters
│   │   • DeleteScheduledJob() - Remove job
│   │   • GetBusinessDate() - Calculate business date
│   │
│   ├── TradeRepository.cs                 # Trade data operations
│   │   • GetTradeListFromTE_CAGS/BAGS/CCAC/SCCF/FRES/FREC()
│   │   • InsertTrades_CAGS/BAGS/CCAC/SCCF/FRES/FREC()
│   │   • GetTradeListFromTE_USTRADE()
│   │   • InsertTrades_USTRADE()
│   │   • GetTradeListFromTE_CDEALER()
│   │   • InsertTrades_CDEALER()
│   │
│   ├── PartyRespository.cs                # Client/Salesperson operations
│   │   • GetPartyListFromGloss() - Extract from GLOSS
│   │   • ReloadAllParties() - Full refresh
│   │
│   ├── InstrumentRepository.cs            # Instrument operations
│   │   • GetInstrumentListFromTE() - Extract instruments
│   │   • InsertInstruments() - Bulk insert
│   │
│   ├── InstrumentPriceRepository.cs       # Price data operations
│   │   • GetInstPriceFromTE() - Extract prices
│   │   • GetInstPriceFXFromTE() - Extract FX prices
│   │   • InsertInstrumentPxInfo() - Insert prices
│   │   • Update_Instrument_ClosePrice() - Update close prices
│   │
│   ├── ClientBalanceRepository.cs         # Balance operations
│   │   • GetClientStockBalanceFromTE()
│   │   • InsertClientStockBalanceInfo()
│   │   • GetClientCashBalanceFromTE()
│   │   • InsertClientCashBalanceInfo()
│   │
│   ├── TradeNarrativeRespository.cs       # Trade narrative operations
│   ├── Trade_Status_Respository.cs        # Trade status operations
│   ├── TradeCancelledRepository.cs        # Cancelled trade operations
│   ├── TradeCharge_AmendedComm_Respository.cs  # Amendment operations
│   ├── AM_Trade_Respository.cs            # AM trade operations
│   ├── ExchangeRateRespository.cs         # FX rate operations
│   ├── HolidayRespository.cs              # Holiday calendar operations
│   └── AppLogRepository.cs                # Application log operations
│
├── ViewModels/                             # MVVM ViewModels
│   ├── MainViewModel.cs                   # Main application ViewModel (910 lines)
│   │   • Window_Loaded() - Initialization
│   │   • Run_AutoMode() - Auto-run mode setup
│   │   • OnExecuteCommand() - Static data jobs (16:00-24:00)
│   │   • OnExecuteCommand1() - EOD trade jobs (00:00-04:00)
│   │   • OnExecuteCommand2() - US trade jobs (05:00-11:00)
│   │   • OnExecuteCommand4() - Central dealer jobs (12:00-23:00)
│   │   • OnAutoRunTimer_Tick() - Timer event handlers
│   │   • LoadScheduledJobs() - Job list refresh
│   │   • LoadAppLogs() - Log list refresh
│   │   • AddAppLog() - Log entry creation
│   │   • OnSelectAllCommand() - Bulk job selection
│   │   • OnEditJobCommand() - Job editing
│   │   • OnAddJobCommand() - Job creation
│   │   • OnDeleteJobCommand() - Job deletion
│   │
│   ├── EditScheduledJobVM.cs              # Job editing ViewModel
│   │   • SaveCommand - Save job changes
│   │   • CancelCommand - Discard changes
│   │   • Parameter management
│   │
│   └── ViewModelLocator.cs                # DI container for ViewModels
│       • Registers ViewModels with MVVM Light SimpleIoc
│       • Provides ViewModel instances to Views
│
├── Views/                                  # WPF Views
│   ├── MainWindow.xaml                    # Main application window
│   │   • Job list with grouping by occurrence
│   │   • Log viewer with auto-scroll
│   │   • Execute/Select All/Auto Run buttons
│   │   • Business date display
│   │
│   ├── EditScheduledJobWindow.xaml        # Job configuration dialog
│   │   • Job details form
│   │   • Parameter grid
│   │   • Time window configuration
│   │
│   └── Controls/                          # Custom WPF Controls
│       ├── GradientHeader.xaml            # Styled header control
│       ├── ScheduledJobExpander.xaml      # Expandable job group
│       └── ScheduledJobView.xaml          # Individual job display
│
├── Images/                                 # UI Resources
│   ├── background.jpg                     # Window background
│   ├── Success.png                        # Success status icon
│   ├── Error.png                          # Error status icon
│   ├── None.png                           # Neutral status icon
│   └── Reports.png                        # Reports icon
│
├── Resources/                              # Application Resources
│   └── Repository.ico                     # Application icon
│
├── Properties/                             # Assembly Properties
│   ├── AssemblyInfo.cs                    # Assembly metadata
│   ├── Resources.resx                     # Embedded resources
│   ├── Settings.settings                  # Application settings
│   └── app.manifest                       # Application manifest
│
├── App.xaml                                # Application definition
├── App.xaml.cs                            # Application code-behind
├── App.config                             # Configuration file
├── packages.config                        # NuGet packages
├── BRPS.csproj                            # Project file
└── Readme.txt                             # Version history
```

### Design Patterns

**1. MVVM (Model-View-ViewModel)**
- **Implementation**: MVVM Light Framework 5.2.0
- **Views**: XAML-based WPF views with data binding
- **ViewModels**: Business logic and state management
- **Models**: Entity Framework entities
- **Communication**: MVVM Light Messenger for cross-ViewModel messaging
- **Commands**: RelayCommand<T> for UI actions
- **Benefits**: Testability, separation of concerns, maintainability

**2. Repository Pattern**
- **Purpose**: Abstract data access logic from business logic
- **Base Class**: RepositoryBase with common CRUD operations
- **Implementations**: Domain-specific repositories (TradeRepository, ClientRepository, etc.)
- **Features**: 
  - Batch operations (2000 records per batch)
  - Transaction management
  - Validation error handling
  - Connection management
- **Benefits**: Testability, reusability, maintainability

**3. Dependency Injection**
- **Container**: MVVM Light SimpleIoc
- **Registration**: ViewModelLocator class
- **Scope**: Singleton ViewModels
- **Injection Points**: ViewModel constructors
- **Benefits**: Loose coupling, testability, flexibility

**4. Command Pattern**
- **Implementation**: RelayCommand and RelayCommand<T>
- **Usage**: All UI actions (Execute, Edit, Delete, etc.)
- **Features**: CanExecute support, parameter passing
- **Benefits**: Decoupling UI from logic, testability

**5. Template Method Pattern**
- **Implementation**: RepositoryBase.SaveChanges()
- **Customization**: Override in derived repositories
- **Common Logic**: Validation error handling, context verification
- **Benefits**: Code reuse, consistency

**6. Factory Pattern**
- **Implementation**: DbProviderFactory for database connections
- **Usage**: GenericDAO connection creation
- **Support**: SQL Server, Sybase ASE
- **Benefits**: Database provider abstraction

**7. Strategy Pattern**
- **Implementation**: Job execution via reflection
- **Strategy Selection**: Job.Process property
- **Execution**: ScheduleJobService.Execute()
- **Benefits**: Extensibility without code changes

**8. Observer Pattern**
- **Implementation**: INotifyPropertyChanged, ObservableCollection
- **Usage**: ViewModel property changes, collection updates
- **Notification**: Automatic UI updates
- **Benefits**: Reactive UI, automatic synchronization

---

## Core Components

### 1. Scheduled Job System

The scheduled job system is the heart of BRPS, providing flexible, configurable, and automated ETL job execution.

#### ScheduledJob Entity Structure

```csharp
public partial class ScheduledJob
{
    public int Job_Id { get; set; }                          // Primary key
    public string Description { get; set; }                  // Human-readable job name
    public Nullable<int> Depend_Job_Id { get; set; }        // Job dependency (not actively used)
    public string Occurence { get; set; }                    // Grouping: "Daily", "EOD", "BOD"
    public int Interval { get; set; }                        // Display order
    public string From_Time { get; set; }                    // Start time (HH:mm format)
    public string End_Time { get; set; }                     // End time (HH:mm format)
    public string Process { get; set; }                      // Method name to invoke
    public int Status { get; set; }                          // 0=Not Started, 1=Success, 2=Error
    public DateTime Business_Date { get; set; }              // Last execution business date
    public Nullable<DateTime> Last_Execution_Time { get; set; } // Last execution timestamp
    public string Message { get; set; }                      // Execution result message
    
    public virtual ICollection<ScheduledJobParam> ScheduledJobParams { get; set; }
}
```

#### ScheduledJobParam Entity

```csharp
public partial class ScheduledJobParam
{
    public int Param_Id { get; set; }                        // Primary key
    public int Job_Id { get; set; }                          // Foreign key to ScheduledJob
    public int Param_Order { get; set; }                     // Parameter sequence
    public string Param_Name { get; set; }                   // Parameter type identifier
    public string Param_Type { get; set; }                   // .NET type (e.g., "System.DateTime")
    public string Param_Value { get; set; }                  // Parameter value or reference
}
```

**Parameter Types:**
- **DATE**: Relative date calculation (e.g., "-1" for yesterday)
- **BUSINESS_DATE**: Uses calculated business date
- **APP_SETTING**: References App.config appSettings key
- **Direct Value**: Literal value with optional type conversion

#### Job Execution Flow

**1. Job Selection Phase:**
```
User/Timer → MainViewModel → Filter Jobs by:
  ├─ Time Window Match (curTime >= From_Time && curTime < End_Time)
  ├─ Execution Status (not already completed for business date)
  ├─ Selection State (manual mode) or Auto Mode flag
  └─ Job Description Match (for specific time windows)
```

**2. Parameter Building Phase:**
```
ScheduleJobService.Execute() → For Each ScheduledJobParam:
  ├─ If Param_Name == "DATE"
  │   └─ Add DateTime.Now.Date.AddDays(Convert.ToInt32(Param_Value))
  ├─ If Param_Name == "BUSINESS_DATE"
  │   └─ Add Business_Date parameter
  ├─ If Param_Name == "APP_SETTING"
  │   ├─ Read ConfigurationManager.AppSettings[Param_Value]
  │   ├─ Convert to Param_Type if specified
  │   └─ Add converted value
  └─ Else (Direct Value)
      ├─ Convert Param_Value to Param_Type if specified
      └─ Add value
```

**3. Method Invocation Phase:**
```
Reflection-Based Execution:
  ├─ Get method: this.GetType().GetMethod(job.Process, Type.GetTypeArray(parameters))
  ├─ Validate method exists
  ├─ Invoke: (int)methodInfo.Invoke(this, parameters.ToArray())
  └─ Return record count or -1 for error
```

**4. Status Update Phase:**
```
Update Job Status:
  ├─ If recordCount < 0
  │   ├─ Set Status = 2 (Error)
  │   └─ Set Message = error details
  ├─ Else If recordCount == 0
  │   ├─ Set Status = 1 (Success)
  │   └─ Set Message = "No record has been saved."
  └─ Else
      ├─ Set Status = 1 (Success)
      └─ Set Message = "The total {recordCount} record(s) has(ve) loaded into system."
      
Update Database:
  ├─ Set Business_Date = current business date
  ├─ Set Last_Execution_Time = DateTime.Now
  └─ Save to database via ScheduledJobRepository.EditScheduledJob()
```

**5. Post-Execution Actions:**
```
Critical Job Completion:
  ├─ If job.Description == "Load Instruments Price" && Status == 1
  │   └─ Environment.Exit(0)  // Terminate application
  ├─ If job.Description == "Load Trades - EOD - CAGS" && Status == 1
  │   └─ Environment.Exit(0)
  ├─ If job.Description == "Load Trades - EOD - USTRADE" && Status == 1
  │   └─ Environment.Exit(0)
  └─ If job.Description == "Load Trades - EOD - CDEALER" && Status == 1
      └─ Environment.Exit(0)
```

#### Time Window Logic

**Time Window Validation:**
```csharp
// Normal window (From_Time < End_Time)
// Example: 16:00 to 24:00
bool inRunTime = (curTime >= From_Time && curTime < End_Time);

// Overnight window (From_Time > End_Time)
// Example: 23:00 to 04:00 (crosses midnight)
bool inRunTime = (curTime >= From_Time || curTime < End_Time);
```

**Manual Execution Override:**
- If user manually executes job outside time window
- System prompts: "The current time is not between scheduled job from {From_Time} to {End_Time}. Are sure to continue?"
- User can confirm to proceed or cancel

#### Job Grouping and Display

**Occurrence Groups:**
- Jobs are grouped by `Occurence` field in UI
- Common values: "Daily", "EOD", "BOD", "Hourly"
- Displayed as expandable sections in MainWindow
- Sorted by `Interval` (display order) then `Job_Id`

#### Example Job Configurations

**Load Instruments Job:**
```
Job_Id: 1
Description: Load Instruments
Process: LoadInstruments
From_Time: 16:00
End_Time: 24:00
Occurence: Daily
Parameters: (none)
```

**Load Trades CAGS Job:**
```
Job_Id: 10
Description: Load Trades - EOD - CAGS
Process: LoadTrades_EOD_CAGS
From_Time: 00:00
End_Time: 04:00
Occurence: EOD
Parameters:
  - Param_Order: 1, Param_Name: BUSINESS_DATE, Param_Type: System.DateTime
```

**Load Clients Job:**
```
Job_Id: 4
Description: Load Clients
Process: LoadClients
From_Time: 16:00
End_Time: 24:00
Occurence: Daily
Parameters:
  - Param_Order: 1, Param_Name: BUSINESS_DATE, Param_Type: System.DateTime
```

### 2. ETL Operations

The ETL (Extract, Transform, Load) operations are the core data processing functions of BRPS, implemented as methods in `ScheduleJobService.cs`.

#### Static Data Jobs (16:00-24:00)

**LoadInstruments()**
```csharp
Purpose: Synchronize instrument master data from Trading Engine
Source: Trading Engine (TE) via stored procedure
Target: BRPS.Instrument table
Process:
  1. Call InstrumentRepository.GetInstrumentListFromTE()
  2. Execute TE stored procedure to extract instruments
  3. Map DataTable to List<Instrument>
  4. Call InstrumentRepository.InsertInstruments(instrList)
  5. Bulk insert/update using Entity Framework
  6. Return count of processed instruments
Typical Volume: 5,000-10,000 instruments
Execution Time: 2-5 minutes
```

**LoadInstruments_Price()**
```csharp
Purpose: Download end-of-day instrument prices and FX rates
Source: Trading Engine (TE)
Target: BRPS.Instrument_Price table, BRPS.Instrument.Close_Price column
Process:
  1. Extract FX prices: GetInstPriceFXFromTE()
  2. Insert FX prices: InsertInstrumentFXPxInfo()
  3. Extract instrument prices: GetInstPriceFromTE()
  4. Insert instrument prices: InsertInstrumentPxInfo()
  5. Update close prices: Update_Instrument_ClosePrice()
  6. Return total count (FX + Instrument prices)
Typical Volume: 5,000-10,000 price records
Execution Time: 3-7 minutes
Critical: Application terminates after successful completion
```

**LoadSalespersons()**
```csharp
Purpose: Synchronize salesperson/remisier data from GLOSS
Source: GLOSS system via stored procedure
Target: BRPS.Party, BRPS.Salesperson tables
Process:
  1. Call PartyRespository.GetPartyListFromGloss(PartyCategory.SALE, 1900-01-01)
  2. Extract all salesperson records (no date filter)
  3. Call PartyRespository.ReloadAllParties(PartyCategory.SALE, parties)
  4. Delete existing salespersons
  5. Bulk insert new salesperson data
  6. Return count of processed salespersons
Typical Volume: 100-500 salespersons
Execution Time: 1-2 minutes
```

**LoadClients(DateTime lastBusinessDate)**
```csharp
Purpose: Synchronize client master data from GLOSS
Source: GLOSS system via stored procedure
Target: BRPS.Party, BRPS.Client, BRPS.ClientAccount tables
Process:
  1. Call PartyRespository.GetPartyListFromGloss(PartyCategory.SECP, 1900-01-01)
  2. Extract all client records (no date filter)
  3. Call PartyRespository.ReloadAllParties(PartyCategory.SECP, parties)
  4. Delete existing clients
  5. Bulk insert new client data with accounts
  6. Return count of processed clients
Typical Volume: 10,000-50,000 clients
Execution Time: 10-30 minutes
Note: Full reload approach, not incremental
```

#### EOD Trade Jobs (00:00-04:00)

**LoadStockBalance()**
```csharp
Purpose: Extract client stock positions from Trading Engine
Source: Trading Engine (TE)
Target: BRPS.Client_StockBalance table
Process:
  1. Call ClientBalanceRepository.GetClientStockBalanceFromTE()
  2. Execute TE stored procedure for stock positions
  3. Map to List<Client_StockBalance>
  4. Call InsertClientStockBalanceInfo(balances)
  5. Truncate existing data
  6. Bulk insert new stock balances
  7. Return count of balance records
Typical Volume: 50,000-200,000 position records
Execution Time: 5-15 minutes
```

**LoadCashBalance()**
```csharp
Purpose: Extract client cash positions from Trading Engine
Source: Trading Engine (TE)
Target: BRPS.Client_CashBalance table
Process:
  1. Call ClientBalanceRepository.GetClientCashBalanceFromTE()
  2. Execute TE stored procedure for cash positions
  3. Map to List<Client_CashBalance>
  4. Call InsertClientCashBalanceInfo(balances)
  5. Truncate existing data
  6. Bulk insert new cash balances
  7. Return count of balance records
Typical Volume: 20,000-100,000 balance records
Execution Time: 3-10 minutes
```

**LoadTradeNarrativeInfo()**
```csharp
Purpose: Extract trade narrative/notes from Trading Engine
Source: Trading Engine (TE)
Target: BRPS.Trade_NarrativeInfo table
Process:
  1. Call TradeNarrativeRespository.GetTradeNarrativeFromTE()
  2. Execute TE stored procedure
  3. Map to List<Trade_NarrativeInfo>
  4. Call InsertTradeNarrativeInfo(narratives)
  5. Insert/update narrative records
  6. Return count of narrative records
Typical Volume: 1,000-5,000 narratives
Execution Time: 1-3 minutes
```

**LoadAmendedTrade()**
```csharp
Purpose: Extract trade commission amendments (past 20 days)
Source: Trading Engine (TE)
Target: BRPS.TradeCharge_AmendedComm table
Process:
  1. Call TradeCharge_AmendedComm_Respository.GetTradeCharge_AmendedComm_FromTE()
  2. Execute TE stored procedure with 20-day lookback
  3. Map to List<TradeCharge_AmendedComm>
  4. Call InsertExtractTradeCharge_AmendedComm_FromTE(amendments)
  5. Insert/update amendment records
  6. Return count of amendments
Typical Volume: 100-1,000 amendments
Execution Time: 1-2 minutes
Enhancement: v1.0.0.5 - Extended from 1 day to 20 days
```

**LoadCancelledTrade(DateTime lastBusinessDate)**
```csharp
Purpose: Extract cancelled trades for business date
Source: Trading Engine (TE)
Target: BRPS.Trade_Cancelled table
Process:
  1. Validate lastBusinessDate parameter
  2. Call TradeCancelledRepository.GetTradeListFromTE(lastBusinessDate)
  3. Execute TE stored procedure for cancelled trades
  4. Map to List<Trade_Cancelled>
  5. Call InsertTrades(cancelledTrades)
  6. Insert cancelled trade records
  7. Return count of cancelled trades
Typical Volume: 10-100 cancellations per day
Execution Time: <1 minute
```

**LoadTrades_EOD_CAGS(DateTime lastBusinessDate)**
```csharp
Purpose: Extract CAGS (Central Assisted General Settlement) trades
Source: Trading Engine (TE)
Target: BRPS.Trade, BRPS.TradeCharge tables
Process:
  1. Validate lastBusinessDate parameter
  2. Call TradeRepository.GetTradeListFromTE_CAGS(lastBusinessDate)
  3. Execute TE stored procedure SP_GET_TRADES_CAGS
  4. Map DataTable to List<Trade> with TradeCharge
  5. Call InsertTrades_CAGS(tradeList, lastBusinessDate)
  6. Begin transaction
  7. For each trade:
     - Check if exists (Business_Date + Trade_No)
     - Update if exists, Insert if new
     - Insert/Update TradeCharge
  8. Commit transaction
  9. Return count of processed trades
Typical Volume: 5,000-20,000 trades
Execution Time: 10-30 minutes
Critical: Application terminates after successful completion
Logging: Extensive logging with +/- method entry/exit
```

**LoadTrades_EOD_BAGS(DateTime lastBusinessDate)**
```csharp
Purpose: Extract BAGS (Broker Assisted General Settlement) trades
Similar to CAGS but for manually settled trades
Typical Volume: 1,000-5,000 trades
Execution Time: 5-15 minutes
```

**LoadTrades_EOD_CCAC(DateTime lastBusinessDate)**
```csharp
Purpose: Extract CCAC (Central Counterparty Clearing) trades
Similar to CAGS but for CCP-cleared trades
Typical Volume: 2,000-10,000 trades
Execution Time: 5-20 minutes
```

**LoadTrades_EOD_SCCF(DateTime lastBusinessDate)**
```csharp
Purpose: Extract SCCF (Securities Clearing and Settlement) trades
Similar to CAGS but for standard clearing
Typical Volume: 3,000-15,000 trades
Execution Time: 7-25 minutes
```

**LoadTrades_EOD_FRES(DateTime lastBusinessDate)**
```csharp
Purpose: Extract FRES (Foreign Exchange Settlement) trades
FX trade settlements
Typical Volume: 500-2,000 trades
Execution Time: 2-10 minutes
```

**LoadTrades_EOD_FREC_OPEN(DateTime lastBusinessDate)**
```csharp
Purpose: Extract FREC (Foreign Exchange Clearing) open trades
FX cleared trades with open positions
Typical Volume: 300-1,500 trades
Execution Time: 2-8 minutes
```

**LoadTrades_EOD_FREC_SETTLED(DateTime lastBusinessDate)**
```csharp
Purpose: Extract FREC settled trades
FX cleared trades that have settled
Typical Volume: 300-1,500 trades
Execution Time: 2-8 minutes
```

#### US Trade Jobs (05:00-11:00)

**LoadTrades_EOD_USTRADE(DateTime lastBusinessDate)**
```csharp
Purpose: Extract US market trades
Source: Trading Engine (TE)
Target: BRPS.Trade, BRPS.TradeCharge tables
Process:
  1. Validate lastBusinessDate parameter
  2. Call TradeRepository.GetTradeListFromTE_USTRADE(lastBusinessDate)
  3. Execute TE stored procedure for US trades
  4. Handle Monday special case: -3 days instead of -1 day
  5. Map to List<Trade>
  6. Call InsertTrades_USTRADE(tradeList, lastBusinessDate)
  7. Insert/Update trades and charges
  8. Return count of processed trades
Typical Volume: 1,000-5,000 trades
Execution Time: 5-15 minutes
Critical: Application terminates after successful completion
Enhancement: v1.0.0.4 - Monday logic (T-3 instead of T-1)
```

#### Central Dealer Jobs (12:00-23:00)

**LoadTrades_EOD_CDEALER(DateTime lastBusinessDate)**
```csharp
Purpose: Extract central dealer proprietary trades
Source: Trading Engine (TE)
Target: BRPS.Trade, BRPS.TradeCharge tables
Process:
  1. Validate lastBusinessDate parameter
  2. Call TradeRepository.GetTradeListFromTE_CDEALER(lastBusinessDate)
  3. Execute TE stored procedure for dealer trades
  4. Map to List<Trade>
  5. Call InsertTrades_CDEALER(tradeList, lastBusinessDate)
  6. Insert/Update trades and charges
  7. Return count of processed trades
Typical Volume: 500-2,000 trades
Execution Time: 3-10 minutes
Critical: Application terminates after successful completion
```

#### BOD (Begin of Day) Jobs (08:45-23:30)

**LoadTrades_BOD(DateTime lastBusinessDate)**
```csharp
Purpose: Load intraday trade status updates
Source: Trading Engine (TE)
Target: BRPS.Trade_Status, BRPS.Trade tables
Process:
  1. Check time window: 08:45 to 23:30
  2. Load trade status: Trade_Status_Respository.GetTradeListFromTE()
  3. Insert status updates (1-month rolling window)
  4. Load AM trades: AM_Trade_Respository.GetTradeListFromTE()
  5. Insert AM trades (FREC, EPSG, REC via SGX API)
  6. Return count of processed trades
Typical Volume: 1,000-10,000 status updates
Execution Time: 5-20 minutes
Enhancement: v1.0.0.13 - Reduced to 1-month records for performance
```

#### Data Transformation Logic

**Common Transformation Patterns:**

1. **DataTable to Entity Mapping:**
```csharp
foreach (DataRow row in dataTable.Rows)
{
    var trade = new Trade
    {
        Business_Date = Convert.ToDateTime(row["Business_Date"]),
        Trade_No = Convert.ToInt64(row["Trade_No"]),
        Trade_Ref = row["Trade_Ref"].ToString(),
        // ... 60+ fields mapped
    };
    tradeList.Add(trade);
}
```

2. **Batch Processing:**
```csharp
// Process in batches of 2000 records
int batchSize = 2000;
int totalRecords = entities.Count;
int batches = (totalRecords / batchSize) + (totalRecords % batchSize > 0 ? 1 : 0);

for (int i = 0; i < batches; i++)
{
    int skip = i * batchSize;
    int take = (i < batches - 1) ? batchSize : (totalRecords % batchSize);
    var batch = entities.Skip(skip).Take(take).ToList();
    
    Context.Set<TEntity>().AddRange(batch);
    Context.SaveChanges();
}
```

3. **Upsert Logic:**
```csharp
var existingTrade = context.Trades
    .FirstOrDefault(t => t.Business_Date == trade.Business_Date 
                      && t.Trade_No == trade.Trade_No);

if (existingTrade != null)
{
    // Update existing
    context.Entry(existingTrade).CurrentValues.SetValues(trade);
}
else
{
    // Insert new
    context.Trades.Add(trade);
}
```

4. **Error Handling:**
```csharp
try
{
    // ETL operation
    return recordCount;
}
catch (Exception ex)
{
    Console.WriteLine(ex.Message);
    Message = string.Format(EXCEPTION_MESSAGE, "Job Name", ex.Message);
    m_logger.Error(Message);
    return -1;  // Indicates error
}
```

### 3. Auto-Run Mode

The system supports automated execution with four distinct time-based modes:

```csharp
// Mode 1: Static Data (16:00-24:00)
// Mode 2: EOD Trades (00:00-04:00)
// Mode 3: US Trades (05:00-11:00)
// Mode 4: Central Dealer (12:00-23:00)
```

Each mode runs on a 30-second timer interval, checking for pending jobs and executing them automatically.

---

## Key Features

### 1. Job Management UI
- View all scheduled jobs grouped by occurrence
- Select/deselect jobs for manual execution
- Edit job configurations (time windows, parameters)
- Add/delete scheduled jobs
- Real-time status updates

### 2. Logging System
- Application log viewer (last 100 entries)
- Status indicators (INFO, ERR)
- Timestamp tracking
- Auto-scroll to latest log entry
- Integration with KGILog library

### 3. Business Date Management
- Configurable cutoff time (default: 06:00)
- Automatic business date calculation
- Handles weekend and holiday logic

### 4. Error Handling
- Comprehensive exception catching
- Detailed error messages
- Job status tracking (Success/Error)
- Timeout configuration (3600 seconds for long-running queries)

---

## Data Model

### Core Entities

**ScheduledJob**
- Job_Id, Description, Process (method name)
- From_Time, End_Time, Interval
- Status, Business_Date, Last_Execution_Time
- Message (execution result)

**Trade**
- Comprehensive trade details (60+ fields)
- Trade_No, Trade_Ref, Client_Number
- Instrument, Qty, Price, Trade_Curr
- Settlement information
- Version tracking

**Client**
- Client_Number, identification details
- Personal/corporate information
- Account status and type
- Risk profile and compliance data

**Instrument**
- Financial instrument master data
- Pricing information
- Market classification

**Party**
- Salesperson and client party information
- Hierarchical relationships

---

## Configuration

### App.config Settings

The application configuration is managed through `App.config` with multiple environment-specific settings.

#### Application Settings

```xml
<appSettings>
  <!-- Auto-run mode control -->
  <add key="AutoRun" value="False"/>
  <!-- Enables/disables automatic job execution on startup -->
  <!-- Values: True, False -->
  <!-- Default: False (manual mode) -->
  
  <!-- Late booking feature -->
  <add key="RunLateBooking" value="True"/>
  <!-- Enables late booking file processing -->
  <!-- Values: True, False -->
  
  <add key="LateBookingFile" value="C:\temp\ForSharing\LateBooking_{0:yyyyMMdd}.txt"/>
  <!-- Late booking file path with date placeholder -->
  <!-- {0:yyyyMMdd} is replaced with current date -->
</appSettings>
```

#### Connection Strings

**Multiple Environment Support:**

The application supports multiple deployment environments with separate connection strings:

**1. Trading Engine (TE) Connections - Sybase ASE:**

```xml
<!-- Production GLOSS -->
<add name="TEConnectionString" 
     connectionString="server=hkgloss6db.sg.kgi.com;port=4500;
                      user id=SG_APP_DEV;password=sgappdevk9i;
                      initial catalog=gls_prd_t;persist security info=True;
                      quotedidentifier=1;enabletracing=0;dynamic_prepare=True" 
     providerName="Sybase.Data.AseClient"/>

<!-- BDM Environment -->
<add name="BDM_TEConnectionString" 
     connectionString="server=10.83.16.15;port=4500;
                      user id=gloss;password=glosstst2;
                      initial catalog=gls_prd_t;..." 
     providerName="Sybase.Data.AseClient"/>

<!-- MIT Environment -->
<add name="MIT_TEConnectionString" 
     connectionString="server=10.83.16.11;port=4500;
                      user id=gloss;password=glossuser;
                      initial catalog=gls_prd_t;..." 
     providerName="Sybase.Data.AseClient"/>

<!-- PP (Pre-Production) Environment -->
<add name="PP_TEConnectionString" 
     connectionString="server=10.82.13.10;port=4500;
                      user id=SG_APP_DEV;password=sgappdevk9i;
                      initial catalog=gls_prd_t;..." 
     providerName="Sybase.Data.AseClient"/>
```

**2. BRPS Database Connections - SQL Server:**

```xml
<!-- Active Connection (GLS Environment) -->
<add name="BRPSDbContext" 
     connectionString="metadata=res://*/Model.BRPSModel.csdl|
                      res://*/Model.BRPSModel.ssdl|
                      res://*/Model.BRPSModel.msl;
                      provider=System.Data.SqlClient;
                      provider connection string=&quot;
                      Integrated Security=SSPI;
                      Persist Security Info=False;
                      Initial Catalog=BRPS_LateBooking;
                      Data Source=preprod;
                      MultipleActiveResultSets=True;
                      App=EntityFramework&quot;" 
     providerName="System.Data.EntityClient"/>

<!-- PP Environment -->
<add name="PP_BRPSDbContext" 
     connectionString="...data source=SGCO-PTSP001RPT;
                      initial catalog=BRPS;
                      user id=brpsUser;password=12345678;..." 
     providerName="System.Data.EntityClient"/>

<!-- GLS Environment -->
<add name="GLS_BRPSDbContext" 
     connectionString="...data source=sgho-ptsu001wapp;
                      initial catalog=BRPS;
                      user id=brpsUser;password=12345678;..." 
     providerName="System.Data.EntityClient"/>

<!-- MIT Environment -->
<add name="MIT_BRPSDbContext" 
     connectionString="...data source=SGHO-PTSS001WAPP;
                      initial catalog=BRPS;
                      user id=brpsUser;password=12345678;..." 
     providerName="System.Data.EntityClient"/>
```

**Connection String Components:**

- **metadata**: Entity Framework model metadata paths (CSDL, SSDL, MSL)
- **provider**: Database provider (System.Data.SqlClient for SQL Server)
- **provider connection string**: Actual database connection parameters
  - **Data Source**: Server name or IP address
  - **Initial Catalog**: Database name
  - **User ID/Password**: SQL authentication (or Integrated Security for Windows auth)
  - **MultipleActiveResultSets**: Enables MARS for concurrent queries
  - **App**: Application name for connection tracking

#### Log4Net Configuration

```xml
<log4net>
  <appender name="RollingLogFileAppender" 
            type="log4net.Appender.RollingFileAppender">
    <!-- Log file location -->
    <file value="Logs/BRPS_ETL.log"/>
    
    <!-- Minimal locking for multi-threaded access -->
    <lockingModel type="log4net.Appender.FileAppender+MinimalLock"/>
    
    <!-- Append to existing file -->
    <appendToFile value="true"/>
    
    <!-- Roll by date -->
    <rollingStyle value="Date"/>
    
    <!-- Date pattern for file naming -->
    <datePattern value="yyyyMMdd"/>
    
    <!-- Log entry format -->
    <layout type="log4net.Layout.PatternLayout">
      <conversionPattern value="%date [%thread] %-5level: %message%newline"/>
    </layout>
  </appender>

  <logger name="BRPS_ETL_Debug">
    <!-- Log level: ALL, DEBUG, INFO, WARN, ERROR, FATAL -->
    <level value="ALL"/>
    <appender-ref ref="RollingLogFileAppender"/>
  </logger>
</log4net>
```

**Log File Naming:**
- Base: `Logs/BRPS_ETL.log`
- Daily rotation: `BRPS_ETL_20171006.log`
- Format: `2017-10-06 14:23:45,123 [1] INFO: LoadInstruments() completed - 5234 records`

#### Entity Framework Configuration

```xml
<entityFramework>
  <defaultConnectionFactory 
      type="System.Data.Entity.Infrastructure.LocalDbConnectionFactory, EntityFramework">
    <parameters>
      <parameter value="v12.0"/>
    </parameters>
  </defaultConnectionFactory>
  
  <providers>
    <provider invariantName="System.Data.SqlClient" 
              type="System.Data.Entity.SqlServer.SqlProviderServices, EntityFramework.SqlServer"/>
  </providers>
</entityFramework>
```

#### Assembly Binding Redirects

```xml
<runtime>
  <assemblyBinding xmlns="urn:schemas-microsoft-com:asm.v1">
    <!-- Entity Framework Mapping API -->
    <dependentAssembly>
      <assemblyIdentity name="EntityFramework.MappingAPI" 
                        publicKeyToken="7ee2e825d201459e" 
                        culture="neutral"/>
      <bindingRedirect oldVersion="0.0.0.0-6.1.0.9" 
                       newVersion="6.1.0.9"/>
    </dependentAssembly>
    
    <!-- Entity Framework Core -->
    <dependentAssembly>
      <assemblyIdentity name="EntityFramework" 
                        publicKeyToken="b77a5c561934e089" 
                        culture="neutral"/>
      <bindingRedirect oldVersion="0.0.0.0-6.0.0.0" 
                       newVersion="6.0.0.0"/>
    </dependentAssembly>
  </assemblyBinding>
</runtime>
```

### Database Connections

The system connects to multiple heterogeneous databases:

#### 1. BRPS Database (SQL Server)

**Purpose**: Local repository for synchronized data

**Schema:**
- **Tables**: 30+ tables including Trade, Client, Instrument, ScheduledJob
- **Views**: Reporting views for data aggregation
- **Stored Procedures**: Business date calculation, data cleanup
- **Functions**: Custom business logic functions

**Key Tables:**
- `ScheduledJob`, `ScheduledJobParam` - Job configuration
- `Trade`, `TradeCharge` - Trade data (millions of records)
- `Client`, `ClientAccount`, `Party` - Client master data
- `Instrument`, `Instrument_Price` - Instrument master and pricing
- `Salesperson` - Sales representative data
- `Client_CashBalance`, `Client_StockBalance` - Position data
- `Trade_Status`, `Trade_NarrativeInfo`, `Trade_Cancelled` - Trade lifecycle
- `AppLog` - Application logging
- `ExchangeRate`, `Holiday`, `Market` - Reference data

**Performance Considerations:**
- Indexed on Business_Date, Trade_No, Client_Number, Instrument
- Partitioning on Business_Date for large tables
- Regular index maintenance and statistics updates
- Query timeout: 3600 seconds for large operations

#### 2. Trading Engine (TE) - Sybase ASE

**Purpose**: Source system for trades, instruments, prices, balances

**Connection:**
- Protocol: Sybase ASE Client
- Port: 4500
- Authentication: SQL authentication
- Connection pooling: Enabled

**Data Extracted:**
- Trades (all settlement types)
- Instruments and prices
- Client balances (cash and stock)
- Trade status updates
- Trade narratives
- Cancelled trades
- Amended commissions

**Stored Procedures Called:**
- `SP_GET_TRADES_CAGS` - CAGS trades
- `SP_GET_TRADES_BAGS` - BAGS trades
- `SP_GET_TRADES_CCAC` - CCAC trades
- `SP_GET_TRADES_SCCF` - SCCF trades
- `SP_GET_TRADES_FRES` - FRES trades
- `SP_GET_TRADES_FREC_OPEN` - FREC open trades
- `SP_GET_TRADES_FREC_SETTLED` - FREC settled trades
- `SP_GET_TRADES_USTRADE` - US trades
- `SP_GET_TRADES_CDEALER` - Central dealer trades
- `SP_GET_INSTRUMENTS` - Instrument master
- `SP_GET_INSTRUMENT_PRICES` - EOD prices
- `SP_GET_CLIENT_STOCK_BALANCE` - Stock positions
- `SP_GET_CLIENT_CASH_BALANCE` - Cash positions

**Query Timeout:**
- Default: 3600 seconds (1 hour)
- Reason: Large data extracts can take 30-45 minutes
- Enhancement: v1.0.0.6 - Increased from default 30 seconds

#### 3. GLOSS (General Ledger and Operations Support System)

**Purpose**: Source system for client and salesperson master data

**Connection:**
- Same Sybase ASE infrastructure as TE
- Different database catalog: `gls_prd_t`

**Data Extracted:**
- Client master data (Party, Client, ClientAccount)
- Salesperson master data (Party, Salesperson)
- Address information
- Account status and limits
- KYC information
- Relationship hierarchies

**Stored Procedures Called:**
- `SP_GET_PARTY_LIST` - Party master data
- `SP_GET_CLIENT_DETAILS` - Client details
- `SP_GET_SALESPERSON_DETAILS` - Salesperson details

### Environment-Specific Configuration

**Deployment Environments:**

1. **Production (PRD)**
   - TE: hkgloss6db.sg.kgi.com
   - BRPS DB: Production SQL Server
   - Auto-run: Enabled
   - Monitoring: 24/7

2. **Pre-Production (PP)**
   - TE: 10.82.13.10
   - BRPS DB: SGCO-PTSP001RPT
   - Auto-run: Configurable
   - Testing: UAT and performance testing

3. **MIT (Market Integration Testing)**
   - TE: 10.83.16.11
   - BRPS DB: SGHO-PTSS001WAPP
   - Auto-run: Configurable
   - Testing: Integration testing

4. **BDM (Business Data Management)**
   - TE: 10.83.16.15
   - BRPS DB: Custom
   - Auto-run: Configurable
   - Testing: Data validation

**Configuration Management:**
- Connection strings commented/uncommented for environment switching
- No configuration transformation (manual config changes)
- Separate builds for each environment
- Version control: Subversion (SVN)

---

## Deployment

**Installation:**
- ClickOnce deployment from network share
- Target: `\\amf-fileserver\home$\zhanghaiping\Apps\`
- Auto-update enabled (7-day check interval)

**Requirements:**
- .NET Framework 4.8
- SQL Server access
- Network connectivity to trading systems
- Windows OS

---

## Version History

**v1.0.0.13 (Oct 6, 2017)**
- Added Status Narrative on ClientAccount Table
- Enhanced LoadTrades_BOD (reduced to 1 month records)
- Enhanced AM_Trade_EPS_Repository.InsertTrades

**v1.0.0.12 (Jun 14, 2017)**
- Added PopulateTradeInfo_SCCF function

**v1.0.0.11 (Jun 8, 2017)**
- Added PopulateTradeInfo_CCAC function

**v1.0.0.10 (Jun 7, 2017)**
- Close_Price column changed from float to decimal

**v1.0.0.8-9 (Apr 11, 2017)**
- Added CCAC, SCCF, FRES, FREC job types

**v1.0.0.7 (Mar 26, 2017)**
- Modified schedule job sequence
- Split CAGS and BAGS functions
- Added time-based job execution (23:00-04:00)

**v1.0.0.6 (Mar 19, 2017)**
- Enhanced timeout (3600 seconds)

**v1.0.0.5 (Mar 13, 2017)**
- Enhanced LoadAmendedTrade (20-day history)

**v1.0.0.4 (Mar 13, 2017)**
- Enhanced LoadTrades_EOD_USTRADE (Monday -3 days logic)

**v1.0.0.3 (Mar 13, 2017)**
- Added Auto Run Mode configuration

---

## Known Limitations

### 1. Hard-Coded Business Logic

**Issue**: Business rules and calculations are embedded directly in code rather than configuration

**Examples:**
- Time window logic: `if (curTime.CompareTo("16:00") > 0 && curTime.CompareTo("24:00") < 0)`
- Business date calculation: `DateTime.Now.AddDays(-1)`
- Monday special case for US trades: `If Monday, set -3 day else set -1 day`
- Batch size: Hard-coded to 2000 records
- Critical job list: Hard-coded job descriptions for auto-termination

**Impact:**
- Changes require code modification and redeployment
- Difficult to adapt to different market requirements
- Testing requires code changes
- No business user control over rules

**Workaround:**
- Use ScheduledJobParam for some parameterization
- Modify App.config for connection strings and basic settings

### 2. Limited Error Recovery

**Issue**: Failed jobs require manual intervention with no automatic recovery

**Specific Problems:**
- No automatic retry on transient failures (network timeouts, deadlocks)
- No circuit breaker pattern for external system failures
- No fallback mechanisms
- Failed jobs remain in error state until manually reset
- No notification system for failures

**Impact:**
- Requires 24/7 monitoring
- Manual intervention needed for recovery
- Potential data gaps if failures go unnoticed
- Increased operational overhead

**Current Mitigation:**
- Timer-based execution provides implicit retry (next 30-second cycle)
- Comprehensive logging for troubleshooting
- Manual job re-execution capability

### 3. No Retry Mechanism

**Issue**: Jobs don't automatically retry on failure with backoff strategy

**Missing Features:**
- No exponential backoff
- No retry count limits
- No dead letter queue for persistent failures
- No partial retry (all-or-nothing approach)

**Impact:**
- Transient failures cause complete job failure
- Network blips require manual intervention
- Database deadlocks not automatically retried
- Increased operational burden

### 4. Single-Threaded Execution

**Issue**: Jobs execute sequentially, not in parallel

**Constraints:**
- One job at a time per time window
- No parallel processing of independent jobs
- Long-running jobs block subsequent jobs
- No job prioritization

**Impact:**
- Longer overall execution time
- Inefficient resource utilization
- Potential for jobs to miss time windows
- Scalability limitations

**Example:**
```
Sequential: Job1 (10 min) → Job2 (15 min) → Job3 (20 min) = 45 minutes
Parallel:   Job1, Job2, Job3 running concurrently = 20 minutes
```

### 5. Legacy Codebase

**Issue**: Last updated in 2017, using older technologies and patterns

**Technology Debt:**
- .NET Framework 4.8 (not .NET Core/.NET 5+)
- Entity Framework 6 (not EF Core)
- WPF (not modern UI frameworks)
- Synchronous operations (no async/await)
- No dependency injection framework (using MVVM Light SimpleIoc)
- No unit tests
- No integration tests

**Impact:**
- Difficult to attract developers familiar with legacy stack
- Missing modern performance optimizations
- Limited cross-platform support (Windows only)
- Harder to maintain and extend

### 6. Full Reload Approach

**Issue**: Many jobs use full reload instead of incremental updates

**Examples:**
- LoadClients: Deletes all clients and reloads
- LoadSalespersons: Deletes all salespersons and reloads
- LoadStockBalance: Truncates and reloads all positions
- LoadCashBalance: Truncates and reloads all balances

**Impact:**
- Longer execution times
- Higher database load
- Potential for data loss if job fails mid-execution
- Inefficient for large datasets

### 7. Limited Monitoring and Alerting

**Issue**: No proactive monitoring or alerting system

**Missing Features:**
- No health checks
- No performance metrics collection
- No alerting on job failures
- No dashboard for operational visibility
- No SLA tracking
- No trend analysis

**Current State:**
- Manual log file review
- UI-based log viewer (last 100 entries)
- No integration with monitoring tools (Nagios, Prometheus, etc.)

### 8. Configuration Management

**Issue**: Environment-specific configuration requires code changes

**Problems:**
- Connection strings in App.config (not encrypted)
- No configuration transformation
- Manual config file editing for environment changes
- Passwords in plain text
- No centralized configuration management

**Impact:**
- Security risk (plain text passwords)
- Deployment complexity
- Configuration drift between environments
- Difficult to manage multiple environments

### 9. Database Performance

**Issue**: Some operations are not optimized for large datasets

**Specific Issues:**
- Full table scans on some queries
- No query optimization for million-row tables
- Batch size (2000) may not be optimal for all scenarios
- No database connection pooling configuration
- Long-running transactions can cause blocking

**Impact:**
- Slow job execution
- Database resource contention
- Potential for timeouts
- Scalability limitations

### 10. Error Handling Granularity

**Issue**: Coarse-grained error handling

**Problems:**
- Single record failure causes entire batch to fail
- No partial success tracking
- No error record isolation
- Limited error detail in logs

**Impact:**
- One bad record can block thousands of good records
- Difficult to identify problematic records
- Manual data correction required

### 11. Dependency on External Systems

**Issue**: Tight coupling to external system availability

**Problems:**
- No offline mode
- No cached data fallback
- No graceful degradation
- Assumes external systems are always available

**Impact:**
- External system downtime blocks all operations
- No ability to continue with partial functionality
- Cascading failures

### 12. Testing Limitations

**Issue**: No automated testing

**Missing:**
- No unit tests
- No integration tests
- No end-to-end tests
- No performance tests
- No regression test suite

**Impact:**
- High risk of introducing bugs
- Difficult to refactor
- Manual testing required for all changes
- No confidence in deployments

---

## Future Considerations

### 1. Modernization to .NET 6/7/8

**Benefits:**
- Cross-platform support (Windows, Linux, macOS)
- Better performance (up to 3x faster)
- Modern language features (C# 10/11)
- Long-term support
- Active development and security updates

**Migration Path:**
1. Upgrade to .NET Framework 4.8 (already done)
2. Migrate to .NET 6 LTS
3. Replace Entity Framework 6 with EF Core 6
4. Update WPF to .NET 6 WPF
5. Modernize dependencies

**Effort**: 3-6 months

### 2. Microservices Architecture

**Proposed Services:**
- **Job Scheduler Service**: Manages job scheduling and execution
- **Trade Service**: Handles all trade-related operations
- **Client Service**: Manages client and salesperson data
- **Instrument Service**: Handles instrument and pricing data
- **Balance Service**: Manages cash and stock balances
- **Notification Service**: Handles alerts and notifications
- **Logging Service**: Centralized logging and monitoring

**Benefits:**
- Independent scaling
- Technology diversity
- Fault isolation
- Easier deployment
- Better team organization

**Challenges:**
- Increased complexity
- Distributed transaction management
- Network latency
- Operational overhead

**Effort**: 6-12 months

### 3. Async/Await Pattern

**Current State**: All operations are synchronous

**Proposed Changes:**
```csharp
// Current
public int LoadInstruments()
{
    var instruments = repository.GetInstrumentListFromTE();
    repository.InsertInstruments(instruments);
    return instruments.Count;
}

// Proposed
public async Task<int> LoadInstrumentsAsync()
{
    var instruments = await repository.GetInstrumentListFromTEAsync();
    await repository.InsertInstrumentsAsync(instruments);
    return instruments.Count;
}
```

**Benefits:**
- Better UI responsiveness
- Improved scalability
- Better resource utilization
- Non-blocking operations

**Effort**: 2-4 months

### 4. Retry Logic with Exponential Backoff

**Proposed Implementation:**
```csharp
public async Task<int> ExecuteWithRetryAsync(Func<Task<int>> operation, int maxRetries = 3)
{
    for (int i = 0; i < maxRetries; i++)
    {
        try
        {
            return await operation();
        }
        catch (TransientException ex)
        {
            if (i == maxRetries - 1) throw;
            
            var delay = TimeSpan.FromSeconds(Math.Pow(2, i));
            await Task.Delay(delay);
        }
    }
    throw new Exception("Max retries exceeded");
}
```

**Benefits:**
- Automatic recovery from transient failures
- Reduced manual intervention
- Better reliability
- Improved uptime

**Effort**: 1-2 months

### 5. Monitoring and Alerting

**Proposed Tools:**
- **Application Insights**: Application performance monitoring
- **Prometheus + Grafana**: Metrics collection and visualization
- **ELK Stack**: Centralized logging (Elasticsearch, Logstash, Kibana)
- **PagerDuty/Opsgenie**: Incident management and alerting

**Metrics to Track:**
- Job execution time
- Job success/failure rate
- Record counts processed
- Database query performance
- External system response times
- Error rates
- Resource utilization (CPU, memory, disk)

**Alerts:**
- Job failures
- Execution time exceeds threshold
- Record count anomalies
- External system unavailability
- Database connection failures

**Effort**: 2-3 months

### 6. Comprehensive Testing Strategy

**Unit Testing:**
- Framework: xUnit or NUnit
- Mocking: Moq
- Coverage target: 80%+
- Focus: Business logic, data transformations

**Integration Testing:**
- Framework: xUnit with TestContainers
- Database: SQL Server in Docker
- External systems: Mock services
- Focus: Repository operations, ETL flows

**End-to-End Testing:**
- Framework: SpecFlow (BDD)
- UI automation: WPF UI testing
- Focus: Complete job execution flows

**Performance Testing:**
- Tool: JMeter or k6
- Scenarios: Large data volumes, concurrent jobs
- Metrics: Throughput, latency, resource usage

**Effort**: 3-4 months

### 7. Configuration Management

**Proposed Approach:**
- **Azure Key Vault** or **HashiCorp Vault**: Secrets management
- **Azure App Configuration**: Centralized configuration
- **Environment variables**: Environment-specific settings
- **Configuration transformation**: Automated config changes per environment

**Benefits:**
- Secure credential storage
- Centralized configuration
- Easy environment management
- Audit trail for configuration changes

**Effort**: 1-2 months

### 8. Parallel Job Execution

**Proposed Implementation:**
```csharp
public async Task ExecuteJobsInParallelAsync(List<ScheduledJob> jobs)
{
    var tasks = jobs
        .Where(j => CanRunInParallel(j))
        .Select(j => ExecuteJobAsync(j));
    
    await Task.WhenAll(tasks);
}
```

**Job Dependency Graph:**
- Identify independent jobs
- Create dependency graph
- Execute independent jobs in parallel
- Respect dependencies

**Benefits:**
- Faster overall execution
- Better resource utilization
- Reduced time windows

**Effort**: 2-3 months

### 9. Incremental Data Loading

**Proposed Approach:**
- Change Data Capture (CDC) on source systems
- Timestamp-based incremental loads
- Merge/upsert instead of delete/insert
- Maintain change history

**Benefits:**
- Faster execution
- Lower database load
- Reduced risk of data loss
- Better scalability

**Effort**: 3-4 months (requires source system changes)

### 10. Modern UI Framework

**Options:**
- **Blazor**: Web-based UI with C#
- **Electron.NET**: Cross-platform desktop
- **MAUI**: Cross-platform mobile and desktop
- **React/Angular + Web API**: Modern web stack

**Benefits:**
- Modern user experience
- Cross-platform support
- Easier to maintain
- Better developer experience

**Effort**: 4-6 months

### 11. Event-Driven Architecture

**Proposed Implementation:**
- **Message Broker**: RabbitMQ, Azure Service Bus, or Kafka
- **Event Sourcing**: Track all state changes as events
- **CQRS**: Separate read and write models

**Benefits:**
- Loose coupling
- Better scalability
- Audit trail
- Real-time processing

**Effort**: 6-9 months

### 12. Cloud Migration

**Target Platform**: Azure or AWS

**Services:**
- **Azure Functions** or **AWS Lambda**: Serverless job execution
- **Azure SQL Database** or **AWS RDS**: Managed database
- **Azure Storage** or **S3**: File storage
- **Azure Monitor** or **CloudWatch**: Monitoring
- **Azure DevOps** or **AWS CodePipeline**: CI/CD

**Benefits:**
- Reduced infrastructure management
- Auto-scaling
- High availability
- Pay-per-use pricing
- Global distribution

**Effort**: 6-12 months

### Priority Roadmap

**Phase 1 (0-6 months): Foundation**
1. Add comprehensive logging (Serilog)
2. Implement retry logic
3. Add basic monitoring and alerting
4. Create unit test suite
5. Implement configuration management

**Phase 2 (6-12 months): Modernization**
1. Migrate to .NET 6
2. Implement async/await
3. Add integration tests
4. Implement parallel job execution
5. Improve error handling

**Phase 3 (12-18 months): Transformation**
1. Microservices architecture
2. Event-driven architecture
3. Cloud migration
4. Modern UI framework
5. Incremental data loading

**Estimated Total Effort**: 18-24 months with a team of 3-4 developers

---

## Support & Maintenance

**Original Developer**: Zhang Haiping (based on deployment path)
**Technology**: Legacy .NET Framework application
**Status**: Production system (as of 2017)

---

*This documentation was generated through code analysis on November 21, 2025*

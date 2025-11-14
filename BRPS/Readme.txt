version - 1.0.0.3
Date - 13 Mar 2017
Description - Added a new configuration parameter() to enable/disable Auto Rund Mode(Run_AutoMode).

version - 1.0.0.4
Date - 13 Mar 2017
Description - Enhanced function => LoadTrades_EOD_USTRADE
		       1. If Monday , set -3 day else set -1 day

version - 1.0.0.5
Date - 13 Mar 2017
Description - Enhanced function => LoadAmendedTrade
		       1. Download past 20 days of Trade Amendment

version - 1.0.0.6
Date - 19 Mar 2017
Description - Enhanced Timeout issue.
		      1. cmd.CommandTimeout = 3600000; in GenericDAO.cs

version - 1.0.0.7
Date - 26 Mar 2017
Description - (1) Modify Schedule Job Sequence
			  BRPS Windows Schedule
				- Added new schedule job to run at 23:05 PM.
			  BRPS Database
		      - Created new SQL Agent job => BRPS_ETL_JOB_SET_STATUS_EOD_STATIC_DATA where static data job to run before Gloss Day End close
			  - BRPS_ETL_JOB_SET_STATUS_EOD_TRADE will be executed once Gloss Day End close complete as usual
			  BRPS ETL Program
			  - Job 24,25,26 run from 23:00 to 04:00
			  (a) MainViewModel.cs => Run_AutoMode() =>  if (curTime.CompareTo("23:00") > 0 && curTime.CompareTo("04:00") < 0
			      Run static data related job before Gloss Day End close

			  (2) Split CAGS and BAGS schedule job functions
					- ScheduleJobService.cs => LoadTrades_EOD_BAGS()
					- ScheduleJobService.cs => LoadTrades_EOD_CAGS()

version - 1.0.0.8
Date - 11 Apr 2017
Description - (1) Added New Job CCAC(ScheduleJobService.cs => LoadTrades_EOD_CCAC())
			  (2) Added New Job SCCF(ScheduleJobService.cs => LoadTrades_EOD_SCCF())

version - 1.0.0.9
Date - 11 Apr 2017
Description - (1) Added New Job CCAC(ScheduleJobService.cs => LoadTrades_EOD_FRES())
			  (2) Added New Job SCCF(ScheduleJobService.cs => LoadTrades_EOD_FREC())

version - 1.0.0.10
Date - 07 Jun 2017
Description - (1) Close_Price column scheme changed from float to decimal in Instrument table

version - 1.0.0.11
Date - 08 Jun 2017
Description - (1) Added new function BRPS.Repositories.PopulateTradeInfo_CCAC()

version - 1.0.0.12
Date - 14 Jun 2017
Description - (1) Added new function BRPS.Repositories.PopulateTradeInfo_SCCF()

version - 1.0.0.13
Date - 06 Oct 2017
Description - (1) Added Status Narrative on ClientAccount Table
			  (2) Enhanced LoadTrades_BOD => (a) Trade Status(reduced to 1 Month records) (b) Enhanced AM_Trade_EPS_Respository.InsertTrades 
					   
 
			  
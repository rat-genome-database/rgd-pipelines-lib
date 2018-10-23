Version 1.0.7 as of Jun 30, 2017
  -java version upgraded to 1.8; logging changed to log4j2

Version 1.0.6 as of Jun 30, 2017
  -when Exception is thrown, instead of calling System.exit(), all running threads are shut down
  and exceptions are thrown; that allows for much more controlled handling of unexpected state

Version 1.0.5.5 as of Jan 26, 2016
  -added log4j support for PipelineSession.dumpCounters(Log), PipelineManager.dumpCounters(Log)

Version 1.0.5.4 as of Dec 11, 2013
  -PipelineRecord can 'unset' a flag
  -PipelineWorkgroup can postpone processing of the current record (the current record is moved
   to end of the current processing queue to be processed at the later time)

Version 1.0.5.3 as of June 19, 2012
  fix: makes PipelineSession object available always; not AFTER a first pipeline workgroup has been added

Version 1.0.5.2 as of June 15, 2012
  fixed bugs with put-back queue; much improved handling of 'registered user exceptions'

Version 1.0.5.1 as of June 12, 2012
added PipelineSession.dumpCounters(Log), PipelineManager.dumpCounters(Log)

Version 1.0.5.0 as of June 7, 2012
RecordProcessorBase (and derived classes RecordPreProcessor and RecordProcessor)
  got new methods: onInit(), called before processing of the first record
  and onExit(), called after processing of the last record.
  New functionality will allow for custom initialization and finalization of a pipeline workgroup.
added PipelineSession.dumpCounters()

Version 1.0.4 as of April 18, 2012 contains class RgdObjectSyncer,
to be used as a base for efficient synchronization of incoming data against RGD database.
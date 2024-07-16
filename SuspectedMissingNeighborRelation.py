from commonFunctions import CommonFunctions
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit, lag, collect_set, length
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as func
import numpy as np
import pandas as pd
import time

start_time = time.time()

com = CommonFunctions("/home/spark-master/Public/JDBC/postgresql-42.5.0.jar")
cols = [
    {"columnName": "Time", "dataTypeName": "Nullable(DateTime)", "isNullable": True},
    {"columnName": "WorkOrderId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "Device", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "NetworkModeId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "BandId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "CarrierId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "SectorId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "DeviceId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "FileId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "DimensionColumnsId", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "Epoch", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "MsgId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "Latitude", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "Longitude", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "S_PCI", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "S_Freq", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "T_PCI", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "T_Freq", "dataTypeName": "Nullable(int32)", "isNullable": True}
]

print('\nStarted!\n')

try:
    class SMNbrRelation:
        def __init__(self):
            # gdevice = com.getDevices(s_device)
            # print('its gdevice: ', gdevice)
            # gdevice = gdevice[0]
            # print('its gdevice: ', gdevice)

            self.df_Signalling = com.readData("Signalling",
                                              ['Time', 'WorkOrderId', 'Device', 'NetworkModeId', 'BandId', 'CarrierId', 'SectorId', 'DeviceId', 'FileId',
                                               'DimensionColumnsId', 'Epoch', 'MsgId', 'Latitude', 'Longitude', 'RRC_NR5G_pci', 'RRC_NR5G_frequency',
                                               'RRC_NR5G_event', 'RRC_NR5G_message'], "Time", "asc")
            # self.df_Signalling = self.df_Signalling.filter(self.df_Signalling.Device == gdevice)

        def f_mnrs_an(self):
            df_Signalling = self.df_Signalling
            out_mnrs = df_Signalling.sort('Time')

            out_mnrs = out_mnrs.withColumn('RRC_NR5G_event', func.split(out_mnrs['RRC_NR5G_event'], ' ')[1])
            li1 = ['rrcSetupRequest', 'rrcReconfiguration', 'measurementReport']
            out_mnrs = out_mnrs.filter(out_mnrs['RRC_NR5G_event'].isin(li1))
            out_mnrs = out_mnrs.withColumn('ss', when(out_mnrs['RRC_NR5G_event'] == 'rrcSetupRequest', 1).otherwise(None))
            w1 = Window.partitionBy('WorkOrderId', 'Device').orderBy('Time').rowsBetween(Window.unboundedPreceding, Window.currentRow)
            out_mnrs = out_mnrs.withColumn('session', func.sum(out_mnrs['ss']).over(w1))

            schemaULDCCH = func.schema_of_json(
                "{\"UL-DCCH-Message\":{\"message\":{\"c1\":{\"measurementReport\":{\"criticalExtensions\":{\"measurementReport\":{\"measResults\":{\"measId\":3,\"measResultNeighCells\":{\"measResultListNR\":[{\"MeasResultNR\":{\"measResult\":{\"cellResults\":{\"resultsSSB-Cell\":{\"rsrp\":56}}},\"physCellId\":432}}]},\"measResultServingMOList\":[{\"MeasResultServMO\":{\"measResultServingCell\":{\"measResult\":{\"cellResults\":{\"resultsSSB-Cell\":{\"rsrp\":46,\"rsrq\":48}}},\"physCellId\":825},\"servCellId\":0}},{\"MeasResultServMO\":{\"measResultServingCell\":{\"measResult\":{\"cellResults\":{\"resultsSSB-Cell\":{\"rsrp\":35,\"rsrq\":54}}},\"physCellId\":825},\"servCellId\":2}}]}}}}}}}}")
            out_mnrs = out_mnrs.withColumn("jsonDataULDCCH", func.from_json(out_mnrs["RRC_NR5G_message"], schemaULDCCH))
            out_mnrs = out_mnrs.select('Time', 'WorkOrderId', 'Device', 'NetworkModeId', 'BandId', 'CarrierId', 'SectorId', 'DeviceId', 'FileId',
                                       'DimensionColumnsId',
                                       'Epoch', 'MsgId', 'Latitude', 'Longitude', 'RRC_NR5G_event', 'session', out_mnrs['RRC_NR5G_pci'].alias('S_PCI'),
                                       out_mnrs['RRC_NR5G_frequency'].alias('S_Freq'), 'RRC_NR5G_message',
                                       out_mnrs[
                                           'jsonDataULDCCH.UL-DCCH-Message.message.c1.measurementReport.criticalExtensions.measurementReport.measResults.measResultNeighCells.measResultListNR.MeasResultNR.physCellId']
                                       .alias('measure_T_PCI'),
                                       out_mnrs[
                                           'jsonDataULDCCH.UL-DCCH-Message.message.c1.measurementReport.criticalExtensions.measurementReport.measResults.measId']
                                       .alias('measure_measId'))

            out_mnrs = out_mnrs.withColumn('ccm', when((out_mnrs['RRC_NR5G_event'] == 'measurementReport') &
                                                       (out_mnrs['measure_T_PCI'].isNull()), 'dont').otherwise('take'))
            out_mnrs = out_mnrs.filter(out_mnrs['ccm'] == 'take')

            schemaDLDCCH = func.schema_of_json(
                "{\"DL-DCCH-Message\":{\"message\":{\"c1\":{\"rrcReconfiguration\":{\"criticalExtensions\":{\"rrcReconfiguration\":{\"measConfig\":{\"measGapConfig\":{\"ext1\":{\"gapUE\":{\"release\":null}}},\"measIdToAddModList\":[{\"MeasIdToAddMod\":{\"measId\":2,\"measObjectId\":1,\"reportConfigId\":1}},{\"MeasIdToAddMod\":{\"measId\":3,\"measObjectId\":1,\"reportConfigId\":2}}],\"measIdToRemoveList\":[{\"MeasId\":1},{\"MeasId\":3},{\"MeasId\":8},{\"MeasId\":9}],\"measObjectToAddModList\":[{\"MeasObjectToAddMod\":{\"measObject\":{\"measObjectNR\":{\"offsetMO\":{\"rsrpOffsetCSI-RS\":\"dB0\",\"rsrpOffsetSSB\":\"dB0\",\"rsrqOffsetCSI-RS\":\"dB0\",\"rsrqOffsetSSB\":\"dB0\",\"sinrOffsetCSI-RS\":\"dB0\",\"sinrOffsetSSB\":\"dB0\"},\"quantityConfigIndex\":1,\"referenceSignalConfig\":{\"ssb-ConfigMobility\":{\"deriveSSB-IndexFromCell\":true,\"ssb-ToMeasure\":{\"setup\":{\"shortBitmap\":\"1101\"}}}},\"smtc1\":{\"duration\":\"sf5\",\"periodicityAndOffset\":{\"sf20\":0}},\"ssbFrequency\":129870,\"ssbSubcarrierSpacing\":\"kHz15\"}},\"measObjectId\":1}}],\"measObjectToRemoveList\":[{\"MeasObjectId\":1},{\"MeasObjectId\":2},{\"MeasObjectId\":3},{\"MeasObjectId\":4}],\"quantityConfig\":{\"quantityConfigNR-List\":[{\"QuantityConfigNR\":{\"quantityConfigCell\":{\"csi-RS-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"},\"ssb-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"}}}}]},\"reportConfigToAddModList\":[{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA2\":{\"a2-Threshold\":{\"rsrp\":61},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms40\"}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"r4\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":true,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":1}},{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA3\":{\"a3-Offset\":{\"rsrp\":6},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms640\",\"useAllowedCellList\":false}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"infinity\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":false,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":2}}],\"reportConfigToRemoveList\":[{\"ReportConfigId\":2},{\"ReportConfigId\":4},{\"ReportConfigId\":5}]},\"nonCriticalExtension\":{\"masterCellGroup\":{\"CellGroupConfig\":{\"cellGroupId\":0,\"mac-CellGroupConfig\":{\"bsr-Config\":{\"periodicBSR-Timer\":\"sf10\",\"retxBSR-Timer\":\"sf80\"},\"drx-Config\":{\"setup\":{\"drx-HARQ-RTT-TimerDL\":56,\"drx-HARQ-RTT-TimerUL\":56,\"drx-InactivityTimer\":\"ms100\",\"drx-LongCycleStartOffset\":{\"ms160\":10},\"drx-RetransmissionTimerDL\":\"sl8\",\"drx-RetransmissionTimerUL\":\"sl8\",\"drx-SlotOffset\":0,\"drx-onDurationTimer\":{\"milliSeconds\":\"ms10\"}}},\"ext1\":{\"csi-Mask\":false},\"phr-Config\":{\"setup\":{\"dummy\":false,\"multiplePHR\":false,\"phr-ModeOtherCG\":\"real\",\"phr-PeriodicTimer\":\"sf20\",\"phr-ProhibitTimer\":\"sf20\",\"phr-Tx-PowerFactorChange\":\"dB1\",\"phr-Type2OtherCell\":false}},\"schedulingRequestConfig\":{\"schedulingRequestToAddModList\":[{\"SchedulingRequestToAddMod\":{\"schedulingRequestId\":0,\"sr-ProhibitTimer\":\"ms1\",\"sr-TransMax\":\"n64\"}}]},\"skipUplinkTxDynamic\":false,\"tag-Config\":{\"tag-ToAddModList\":[{\"TAG\":{\"tag-Id\":0,\"timeAlignmentTimer\":\"infinity\"}}]}},\"physicalCellGroupConfig\":{\"ext2\":{},\"p-NR-FR1\":23,\"pdsch-HARQ-ACK-Codebook\":\"dynamic\"},\"rlc-BearerToAddModList\":[{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":1,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":1,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":2,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":3,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":4,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":2,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"kBps8\",\"priority\":8,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size18\",\"t-Reassembly\":\"ms45\",\"t-StatusProhibit\":\"ms20\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"p16\",\"sn-FieldLength\":\"size18\",\"t-PollRetransmit\":\"ms45\"}}}}}],\"spCellConfig\":{\"reconfigurationWithSync\":{\"newUE-Identity\":17267,\"spCellConfigCommon\":{\"dmrs-TypeA-Position\":\"pos2\",\"downlinkConfigCommon\":{\"frequencyInfoDL\":{\"absoluteFrequencyPointA\":129450,\"absoluteFrequencySSB\":129870,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialDownlinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pdcch-ConfigCommon\":{\"setup\":{\"commonSearchSpaceList\":[{\"SearchSpace\":{\"controlResourceSetId\":0,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n0\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n0\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n1\"},\"searchSpaceId\":1,\"searchSpaceType\":{\"common\":{\"dci-Format0-0-AndFormat1-0\":{}}}}}],\"controlResourceSetZero\":0,\"pagingSearchSpace\":0,\"ra-SearchSpace\":1,\"searchSpaceOtherSystemInformation\":1,\"searchSpaceSIB1\":0,\"searchSpaceZero\":0}},\"pdsch-ConfigCommon\":{\"setup\":{\"pdsch-TimeDomainAllocationList\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}}]}}}},\"n-TimingAdvanceOffset\":\"n0\",\"physCellId\":11,\"ss-PBCH-BlockPower\":27,\"ssb-PositionsInBurst\":{\"shortBitmap\":\"0100\"},\"ssb-periodicityServingCell\":\"ms20\",\"ssbSubcarrierSpacing\":\"kHz15\",\"uplinkConfigCommon\":{\"dummy\":\"infinity\",\"frequencyInfoUL\":{\"absoluteFrequencyPointA\":138650,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"p-Max\":23,\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialUplinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pucch-ConfigCommon\":{\"setup\":{\"p0-nominal\":-100,\"pucch-GroupHopping\":\"neither\",\"pucch-ResourceCommon\":0}},\"pusch-ConfigCommon\":{\"setup\":{\"msg3-DeltaPreamble\":0,\"p0-NominalWithGrant\":-98,\"pusch-TimeDomainAllocationList\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":55}}]}},\"rach-ConfigCommon\":{\"setup\":{\"prach-RootSequenceIndex\":{\"l839\":550},\"ra-ContentionResolutionTimer\":\"sf40\",\"rach-ConfigGeneric\":{\"msg1-FDM\":\"one\",\"msg1-FrequencyStart\":15,\"powerRampingStep\":\"dB2\",\"prach-ConfigurationIndex\":18,\"preambleReceivedTargetPower\":-106,\"preambleTransMax\":\"n10\",\"ra-ResponseWindow\":\"sl20\",\"zeroCorrelationZoneConfig\":12},\"restrictedSetConfig\":\"unrestrictedSet\",\"rsrp-ThresholdSSB\":0,\"ssb-perRACH-OccasionAndCB-PreamblesPerSSB\":{\"oneHalf\":\"n64\"}}}}}},\"t304\":\"ms2000\"},\"rlf-TimersAndConstants\":{\"setup\":{\"ext1\":{\"t311\":\"ms3000\"},\"n310\":\"n20\",\"n311\":\"n1\",\"t310\":\"ms2000\"}},\"servCellIndex\":0,\"spCellConfigDedicated\":{\"csi-MeasConfig\":{\"setup\":{\"csi-IM-ResourceSetToAddModList\":[{\"CSI-IM-ResourceSet\":{\"csi-IM-ResourceSetId\":0,\"csi-IM-Resources\":[{\"CSI-IM-ResourceId\":0}]}}],\"csi-IM-ResourceToAddModList\":[{\"CSI-IM-Resource\":{\"csi-IM-ResourceElementPattern\":{\"pattern1\":{\"subcarrierLocation-p1\":\"s0\",\"symbolLocation-p1\":13}},\"csi-IM-ResourceId\":0,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"periodicityAndOffset\":{\"slots40\":0}}}],\"csi-ReportConfigToAddModList\":[{\"CSI-ReportConfig\":{\"carrier\":0,\"codebookConfig\":{\"codebookType\":{\"type1\":{\"codebookMode\":1,\"subType\":{\"typeI-SinglePanel\":{\"nrOfAntennaPorts\":{\"moreThanTwo\":{\"n1-n2\":{\"two-one-TypeI-SinglePanel-Restriction\":\"11111111\"},\"typeI-SinglePanel-codebookSubsetRestriction-i2\":\"11111111 11111111\"}},\"typeI-SinglePanel-ri-Restriction\":\"00001111\"}}}}},\"cqi-Table\":\"table2\",\"csi-IM-ResourcesForInterference\":1,\"ext1\":{},\"groupBasedBeamReporting\":{\"disabled\":{\"nrofReportedRS\":\"n1\"}},\"reportConfigId\":0,\"reportConfigType\":{\"periodic\":{\"pucch-CSI-ResourceList\":[{\"PUCCH-CSI-Resource\":{\"pucch-Resource\":6,\"uplinkBandwidthPartId\":0}}],\"reportSlotConfig\":{\"slots160\":12}}},\"reportFreqConfiguration\":{\"cqi-FormatIndicator\":\"widebandCQI\",\"pmi-FormatIndicator\":\"widebandPMI\"},\"reportQuantity\":{\"cri-RI-PMI-CQI\":null},\"resourcesForChannelMeasurement\":0,\"subbandSize\":\"value1\",\"timeRestrictionForChannelMeasurements\":\"notConfigured\",\"timeRestrictionForInterferenceMeasurements\":\"notConfigured\"}}],\"csi-ResourceConfigToAddModList\":[{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"nzp-CSI-RS-SSB\":{\"nzp-CSI-RS-ResourceSetList\":[{\"NZP-CSI-RS-ResourceSetId\":0}]}},\"csi-ResourceConfigId\":0,\"resourceType\":\"periodic\"}},{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"csi-IM-ResourceSetList\":[{\"CSI-IM-ResourceSetId\":0}]},\"csi-ResourceConfigId\":1,\"resourceType\":\"periodic\"}}],\"nzp-CSI-RS-ResourceSetToAddModList\":[{\"NZP-CSI-RS-ResourceSet\":{\"nzp-CSI-RS-Resources\":[{\"NZP-CSI-RS-ResourceId\":0}],\"nzp-CSI-ResourceSetId\":0}}],\"nzp-CSI-RS-ResourceToAddModList\":[{\"NZP-CSI-RS-Resource\":{\"nzp-CSI-RS-ResourceId\":0,\"periodicityAndOffset\":{\"slots10\":0},\"powerControlOffset\":0,\"powerControlOffsetSS\":\"db0\",\"resourceMapping\":{\"cdm-Type\":\"fd-CDM2\",\"density\":{\"one\":null},\"firstOFDMSymbolInTimeDomain\":13,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"frequencyDomainAllocation\":{\"row4\":\"010\"},\"nrofPorts\":\"p4\"},\"scramblingID\":11}}]}},\"defaultDownlinkBWP-Id\":0,\"firstActiveDownlinkBWP-Id\":0,\"initialDownlinkBWP\":{\"pdcch-Config\":{\"setup\":{\"controlResourceSetToAddModList\":[{\"ControlResourceSet\":{\"cce-REG-MappingType\":{\"nonInterleaved\":null},\"controlResourceSetId\":1,\"duration\":2,\"frequencyDomainResources\":\"11110000 00000000 00000000 00000000 00000000 00000\",\"pdcch-DMRS-ScramblingID\":11,\"precoderGranularity\":\"sameAsREG-bundle\"}}],\"searchSpacesToAddModList\":[{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n2\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":2,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-1-And-1-1\"}}}},{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n4\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":3,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-0-And-1-0\"}}}}]}},\"pdsch-Config\":{\"setup\":{\"dmrs-DownlinkForPDSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\"}},\"maxNrofCodeWordsScheduledByDCI\":\"n1\",\"mcs-Table\":\"qam256\",\"pdsch-TimeDomainAllocationList\":{\"setup\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}},{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":67}}]},\"prb-BundlingType\":{\"staticBundling\":{}},\"rbg-Size\":\"config1\",\"resourceAllocation\":\"resourceAllocationType1\",\"tci-StatesToAddModList\":[{\"TCI-State\":{\"qcl-Type1\":{\"cell\":0,\"qcl-Type\":\"typeA\",\"referenceSignal\":{\"ssb\":1}},\"tci-StateId\":0}}]}}},\"pdsch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":2},\"nrofHARQ-ProcessesForPDSCH\":\"n16\"}},\"tag-Id\":0,\"uplinkConfig\":{\"firstActiveUplinkBWP-Id\":0,\"initialUplinkBWP\":{\"pucch-Config\":{\"setup\":{\"dl-DataToUL-ACK\":[{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4}],\"format2\":{\"setup\":{\"maxCodeRate\":\"zeroDot15\",\"simultaneousHARQ-ACK-CSI\":\"true\"}},\"pucch-PowerControl\":{\"deltaF-PUCCH-f0\":0,\"deltaF-PUCCH-f1\":0,\"deltaF-PUCCH-f2\":2,\"deltaF-PUCCH-f3\":0,\"p0-Set\":[{\"P0-PUCCH\":{\"p0-PUCCH-Id\":1,\"p0-PUCCH-Value\":0}}],\"pathlossReferenceRSs\":[{\"PUCCH-PathlossReferenceRS\":{\"pucch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"resourceSetToAddModList\":[{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":0,\"resourceList\":[{\"PUCCH-ResourceId\":1},{\"PUCCH-ResourceId\":2},{\"PUCCH-ResourceId\":3},{\"PUCCH-ResourceId\":4}]}},{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":1,\"resourceList\":[{\"PUCCH-ResourceId\":5}]}}],\"resourceToAddModList\":[{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":2,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":0,\"startingPRB\":5}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":1,\"startingPRB\":11}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":2,\"startingPRB\":12}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":3,\"startingPRB\":13}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":4,\"startingPRB\":14}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":9,\"nrofSymbols\":1,\"startingSymbolIndex\":12}},\"pucch-ResourceId\":5,\"startingPRB\":4}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":5,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":6,\"startingPRB\":6}}],\"schedulingRequestResourceToAddModList\":[{\"SchedulingRequestResourceConfig\":{\"periodicityAndOffset\":{\"sl10\":9},\"resource\":0,\"schedulingRequestID\":0,\"schedulingRequestResourceId\":1}}]}},\"pusch-Config\":{\"setup\":{\"codebookSubset\":\"nonCoherent\",\"dataScramblingIdentityPUSCH\":11,\"dmrs-UplinkForPUSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\",\"transformPrecodingDisabled\":{\"scramblingID0\":11}}},\"maxRank\":1,\"pusch-PowerControl\":{\"msg3-Alpha\":\"alpha1\",\"p0-AlphaSets\":[{\"P0-PUSCH-AlphaSet\":{\"alpha\":\"alpha1\",\"p0\":0,\"p0-PUSCH-AlphaSetId\":0}}],\"pathlossReferenceRSToAddModList\":[{\"PUSCH-PathlossReferenceRS\":{\"pusch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"pusch-TimeDomainAllocationList\":{\"setup\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":69}}]},\"resourceAllocation\":\"resourceAllocationType1\",\"transformPrecoder\":\"disabled\",\"txConfig\":\"codebook\"}},\"srs-Config\":{\"setup\":{\"srs-ResourceSetToAddModList\":[{\"SRS-ResourceSet\":{\"alpha\":\"alpha1\",\"p0\":-86,\"pathlossReferenceRS\":{\"ssb-Index\":1},\"resourceType\":{\"aperiodic\":{\"aperiodicSRS-ResourceTrigger\":1,\"csi-RS\":1,\"slotOffset\":1}},\"srs-PowerControlAdjustmentStates\":\"separateClosedLoop\",\"srs-ResourceIdList\":[{\"SRS-ResourceId\":0}],\"srs-ResourceSetId\":0,\"usage\":\"codebook\"}}],\"srs-ResourceToAddModList\":[{\"SRS-Resource\":{\"freqDomainPosition\":0,\"freqDomainShift\":0,\"freqHopping\":{\"b-SRS\":0,\"b-hop\":0,\"c-SRS\":0},\"groupOrSequenceHopping\":\"neither\",\"nrofSRS-Ports\":\"port1\",\"resourceMapping\":{\"nrofSymbols\":\"n1\",\"repetitionFactor\":\"n1\",\"startPosition\":0},\"resourceType\":{\"aperiodic\":{}},\"sequenceId\":0,\"spatialRelationInfo\":{\"referenceSignal\":{\"ssb-Index\":1},\"servingCellId\":0},\"srs-ResourceId\":0,\"transmissionComb\":{\"n4\":{\"combOffset-n4\":0,\"cyclicShift-n4\":0}}}}]}}},\"pusch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":1}}}}}}}}},\"radioBearerConfig\":{\"drb-ToAddModList\":[{\"DRB-ToAddMod\":{\"cnAssociation\":{\"sdap-Config\":{\"defaultDRB\":true,\"mappedQoS-FlowsToAdd\":[{\"QFI\":1}],\"pdu-Session\":2,\"sdap-HeaderDL\":\"present\",\"sdap-HeaderUL\":\"present\"}},\"drb-Identity\":1,\"pdcp-Config\":{\"drb\":{\"discardTimer\":\"infinity\",\"headerCompression\":{\"notUsed\":null},\"pdcp-SN-SizeDL\":\"len18bits\",\"pdcp-SN-SizeUL\":\"len18bits\",\"statusReportRequired\":\"true\"},\"t-Reordering\":\"ms140\"},\"recoverPDCP\":\"true\"}}],\"srb-ToAddModList\":[{\"SRB-ToAddMod\":{\"srb-Identity\":1}},{\"SRB-ToAddMod\":{\"srb-Identity\":2}}]}}},\"rrc-TransactionIdentifier\":2}}}}}\n")
            out_mnrs = out_mnrs.withColumn("jsonDataDLDCCH", func.from_json(out_mnrs["RRC_NR5G_message"], schemaDLDCCH))
            out_mnrs = out_mnrs.select('Time', 'WorkOrderId', 'Device', 'NetworkModeId', 'BandId', 'CarrierId', 'SectorId', 'DeviceId', 'FileId',
                                       'DimensionColumnsId',
                                       'Epoch', 'MsgId', 'Latitude', 'Longitude', 'RRC_NR5G_event', 'session', 'S_PCI', 'S_Freq', 'measure_T_PCI', 'measure_measId',
                                       out_mnrs[
                                           'jsonDataDLDCCH.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.nonCriticalExtension.masterCellGroup.CellGroupConfig.spCellConfig.reconfigurationWithSync.spCellConfigCommon.physCellId']
                                       .alias('reconfig_T_PCI'),
                                       out_mnrs[
                                           'jsonDataDLDCCH.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.nonCriticalExtension.masterCellGroup.CellGroupConfig.spCellConfig.reconfigurationWithSync.spCellConfigCommon.downlinkConfigCommon.frequencyInfoDL.absoluteFrequencySSB']
                                       .alias('reconfig_T_Freq'),
                                       out_mnrs[
                                           'jsonDataDLDCCH.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.measIdToAddModList.MeasIdToAddMod.measId']
                                       .alias('reconfig_measId'),
                                       out_mnrs[
                                           'jsonDataDLDCCH.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.measIdToAddModList.MeasIdToAddMod.measObjectId']
                                       .alias('reconfig_measObjectId'),
                                       out_mnrs[
                                           'jsonDataDLDCCH.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.measObjectToAddModList.MeasObjectToAddMod.measObjectId']
                                       .alias('reconfig_ssb_measObjectId'),
                                       out_mnrs[
                                           'jsonDataDLDCCH.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.measObjectToAddModList.MeasObjectToAddMod.measObject.measObjectNR.ssbFrequency']
                                       .alias('reconfig_ssbFrequency')
                                       )

            mnrs_wn = Window.partitionBy('WorkOrderId', 'Device').orderBy('Time')
            out_mnrs = out_mnrs.withColumn('Succ_T_PCI', func.lead('reconfig_T_PCI', 1).over(mnrs_wn)) \
                .withColumn('Succ_T_Freq', func.lead('reconfig_T_Freq', 1).over(mnrs_wn))
            out_mnrs = out_mnrs.withColumn('R_Status', when((out_mnrs['RRC_NR5G_event'] == 'measurementReport') &
                                                            (out_mnrs['Succ_T_PCI'].isNotNull()),
                                                            'Success').otherwise(None))

            success_mnrs = out_mnrs.filter(out_mnrs['R_Status'].isNotNull())
            success_mnrs = success_mnrs.select('WorkOrderId', 'Device', 'S_PCI', 'S_Freq', success_mnrs['Succ_T_PCI'].alias('T_PCI'),
                                               success_mnrs['Succ_T_Freq'].alias('T_Freq'), 'R_Status').distinct()
            success_mnrs = success_mnrs.withColumn('Relation', concat_ws('_', 'WorkOrderId', 'Device', 'S_PCI', 'S_Freq', 'T_PCI', 'T_Freq'))

            fail_mnrs = out_mnrs.filter(out_mnrs['R_Status'].isNull())
            fail_mnrs = fail_mnrs.drop('reconfig_T_PCI', 'reconfig_T_Freq', 'Succ_T_PCI', 'Succ_T_Freq', 'R_Status')
            fail_mnrs = fail_mnrs.sort('Time')
            w3 = Window.partitionBy('WorkOrderId', 'Device', 'session').orderBy('Time').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            fail_mnrs = fail_mnrs.withColumn('nul_sess', func.count(fail_mnrs['measure_measId']).over(w3))
            fail_mnrs = fail_mnrs.filter(fail_mnrs['nul_sess'] > 0)
            fail_mnrs = fail_mnrs.withColumn('ss2', when(fail_mnrs['RRC_NR5G_event'] == 'rrcReconfiguration', 0).otherwise(1))
            w4 = Window.partitionBy('WorkOrderId', 'Device').orderBy('Time').rowsBetween(Window.unboundedPreceding, Window.currentRow)
            fail_mnrs = fail_mnrs.withColumn('session_N', func.sum(fail_mnrs['ss2']).over(w4))
            fail_mnrs = fail_mnrs.drop('session', 'nul_sess', 'ss2')

            w5 = Window.partitionBy('WorkOrderId', 'Device').orderBy('Time')
            fail_mnrs = fail_mnrs.withColumn('measure_measId_new', func.lead('measure_measId', 1).over(w5))
            w6 = Window.partitionBy('WorkOrderId', 'Device', 'session_N').orderBy('Time').rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
            fail_mnrs = fail_mnrs.withColumn('measure_measId_new', func.last('measure_measId_new', ignorenulls=True).over(w6))
            fail_mnrs = fail_mnrs.drop('measure_measId')
            fail_mnrs = fail_mnrs.withColumn('m_ob', fail_mnrs['reconfig_measObjectId'][(func.expr('array_position(reconfig_measId, measure_measId_new)')) - 1])
            fail_mnrs = fail_mnrs.withColumn('m_ob', func.last('m_ob', ignorenulls=True).over(w6))
            fail_mnrs = fail_mnrs.withColumn('ssbFrequency', fail_mnrs['reconfig_ssbFrequency'][(func.expr('array_position(reconfig_ssb_measObjectId, m_ob)')) - 1])
            fail_mnrs = fail_mnrs.withColumn('T_Freq', func.last('ssbFrequency', ignorenulls=True).over(w6))
            fail_mnrs = fail_mnrs.withColumn('T_Freq', func.lag(fail_mnrs['T_Freq'], 1).over(w5))
            fail_mnrs = fail_mnrs.filter(fail_mnrs['RRC_NR5G_event'] == 'measurementReport')

            fail_mnrs = fail_mnrs.select('Time', 'WorkOrderId', 'Device', 'NetworkModeId', 'BandId', 'CarrierId', 'SectorId', 'DeviceId', 'FileId',
                                         'DimensionColumnsId', 'Epoch', 'MsgId', 'Latitude', 'Longitude', 'S_PCI', 'S_Freq',
                                         func.explode(fail_mnrs['measure_T_PCI']).alias('T_PCI'), 'T_Freq')

            fail_mnrs = fail_mnrs.filter(fail_mnrs['S_PCI'] != fail_mnrs['T_PCI'])
            w12 = Window.partitionBy('WorkOrderId', 'Device').orderBy('Time')
            fail_mnrs = fail_mnrs.withColumn('T_Freq', func.last('T_Freq', ignorenulls=True).over(w12))
            fail_mnrs = fail_mnrs.withColumn('Relation', concat_ws('_', 'WorkOrderId', 'Device', 'S_PCI', 'S_Freq', 'T_PCI', 'T_Freq'))
            fail_mnrs = fail_mnrs.join(success_mnrs, ['Relation'], 'left_anti')
            fail_mnrs = fail_mnrs.drop('Relation')
            fail_mnrs = fail_mnrs.sort('Time')

            return fail_mnrs


    o_final_df = SMNbrRelation().f_mnrs_an()

    # o_final_df.show()
    # o_final_df.write.mode('overwrite').options(header='True').csv('/home/spark-master/Public/Raheel/final/smnr')

    pdFrame = o_final_df.toPandas()
    pdFrame["Time"] = pdFrame["Time"].dt.strftime("%Y-%m-%d %X.%f")
    pdFrame = pdFrame.replace({np.nan: None})
    com.output(pdFrame, "py_SuspectedMissingNeighborRelation", cols)
except Exception as ex:
    com.sendProcessingErrorNotification(ex.__str__())
print("\n--- Query process time is %s minutes ---\n" % ((time.time() - start_time) / 60))





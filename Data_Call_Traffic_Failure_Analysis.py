from commonFunctions import CommonFunctions
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit, lag, collect_set, length
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as func
import numpy as np
#from pyspark.sql.types import StringType
import time
start_time = time.time()


com = CommonFunctions("/home/spark-master/Public/JDBC/postgresql-42.5.0.jar")
# com = CommonFunctions()
cols = [
    {"columnName": "Failed_Event_Number", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "Failed_Event_Type", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "Event_Analysis_Start_Time", "dataTypeName": "Nullable(DateTime)", "isNullable": True},
    {"columnName": "Event_Analysis_End_Time", "dataTypeName": "Nullable(DateTime)", "isNullable": True},
    {"columnName": "Time", "dataTypeName": "Nullable(DateTime)", "isNullable": True},
    {"columnName": "WorkOrderId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "NetworkModeId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "BandId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "CarrierId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "SectorId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "Device", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "DeviceId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "FileId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "DimensionColumnsId", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "Epoch", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "MsgId", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "Latitude", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "Longitude", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "NR5G_PCC_NARFCN", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "NR5G_PCC_PCI", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "NR5G_PCC_RSRP_Avg", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "NR5G_PCC_RSRP_Min", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "NR5G_PCC_SINR_Avg", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "NR5G_PCC_SINR_Min", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "MCC_MNC", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "NR5G_PCC_PCI_Last", "dataTypeName": "Nullable(String)", "isNullable": True},
    {"columnName": "NR5G_PCC_RSRP_Last", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "NR5G_PCC_SINR_Last", "dataTypeName": "Nullable(int32)", "isNullable": True},
    {"columnName": "Analysis", "dataTypeName": "Nullable(String)", "isNullable": True}

]


print('\nStarted!\n')


def read_all_tables_remote(s_device):
    gdevice = com.getDevices(s_device)
    print('its gdevice: ', gdevice)
    gdevice = gdevice[0]
    print('its gdevice: ', gdevice)

    df_event_NR = com.readData("Event_NR", ['Time', 'WorkOrderId', 'Device', 'Event_CallEvent'], "Time", "asc")
    df_event_NR = df_event_NR.filter(df_event_NR.Device == gdevice)

    df_MEAS_NR = com.readData("MEAS_NR", ['Time', 'WorkOrderId', 'Device', 'MEAS_NR5G_PCC_NARFCN', 'MEAS_NR5G_PCC_PCI',
                                          'MEAS_NR5G_PCC_RSRP_AVG', 'MEAS_NR5G_PCC_SNR_AVG'], "Time", "asc")
    df_MEAS_NR = df_MEAS_NR.filter(df_MEAS_NR.Device == gdevice)

    df_Signalling = com.readData("Signalling", ['Time', 'WorkOrderId', 'Device', 'RRC_NR5G_event', 'RRC_NR5G_pci',
                                                'RRC_NR5G_message'], "Time", "asc")
    df_Signalling = df_Signalling.filter(df_Signalling.Device == gdevice)

    df_RACH_NR = com.readData("RACH_NR", ['Time', 'WorkOrderId', 'Device', 'RACHT_NR5G_CRNTI', 'RACHT_NR5G_RACHReason',
                                          'RACHA_NR5G_numAttmpt', 'RACHA_NR5G_RACHResult'], "Time", "asc")
    df_RACH_NR = df_RACH_NR.filter(df_RACH_NR.Device == gdevice)

    df_rc = com.readData("Event_NR",
                         ['Time', 'WorkOrderId', 'NetworkModeId', 'BandId', 'CarrierId', 'SectorId', 'Device',
                          'DeviceId', 'FileId', 'DimensionColumnsId', 'Epoch', 'MsgId', 'Latitude', 'Longitude'],
                         "Time", "asc")
    df_rc = df_rc.filter(df_rc.Device == gdevice)

    return df_event_NR, df_MEAS_NR, df_Signalling, df_RACH_NR, df_rc


def an_window(df_event_NR):
    """
    This function return analysis time window.

    :param df_event_NR:
    :return: failswindow
    """
    Condition_Window = (df_event_NR.Event_CallEvent == '"Setup Start"') | \
                       (df_event_NR.Event_CallEvent == '"Call End"') | (
                           df_event_NR.Event_CallEvent.contains('Traffic Fail'))
    df1 = df_event_NR.select('WorkOrderId', 'Time', 'Event_CallEvent').filter(Condition_Window).sort(df_event_NR.Time.asc())
    partition = Window.partitionBy('WorkOrderId').orderBy('Time')
    df1 = df1.withColumn("Event_Analysis_Start_Time", lag("Time", 1).over(partition))
    failswindow = df1.filter(df1.Event_CallEvent.contains('Traffic Fail'))
    failswindow = failswindow.withColumn("Failed_Event_Number", monotonically_increasing_id() + 1)
    failswindow = failswindow.select(failswindow.Event_CallEvent.alias('Failed_Event_Type'), 'Event_Analysis_Start_Time', failswindow.Time.alias('Event_Analysis_End_Time'), 'Failed_Event_Number')
    return failswindow


def event_nr_msgs(df_event_NR):
    """
    This function return all the messages in analysis time window.

    :param df_event_NR:
    :return:
    """
    an_w = an_window(df_event_NR)
    Condition_event_NR = (df_event_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_event_NR["Time"] <= an_w["Event_Analysis_End_Time"])
    out_event_NR = an_w.join(df_event_NR, Condition_event_NR, 'leftouter')
    out_event_NR = out_event_NR.drop('Failed_Event_Type', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time')
    out_event_NR = out_event_NR.sort(out_event_NR.Time.asc())
    return out_event_NR


def f_meas_nr(df_MEAS_NR):
    """
    This function return these tags
    1- (when RSRP_Avg < -110, "Poor RF condition")
    2- (when SINR_Avg < 0, "Poor RF condition")
    3- (when RSRP_Avg.isNull() and SINR_Avg.isNull(), 'Lost UE')

    :param df_MEAS_NR:
    :return out_MEAS_NR:
    """
    an_w = an_window(df_event_NR)
    Condition_MEAS_NR = (df_MEAS_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_MEAS_NR["Time"] <= an_w["Event_Analysis_End_Time"])
    out_MEAS_NR = an_w.join(df_MEAS_NR, Condition_MEAS_NR, 'leftouter').sort(df_MEAS_NR.Time.asc())
    out_MEAS_NR = out_MEAS_NR.drop('Failed_Event_Type')
    out_MEAS_NR = out_MEAS_NR.groupby('Failed_Event_Number').agg(
        func.expr('collect_set(MEAS_NR5G_PCC_NARFCN)').alias('NR5G_PCC_NARFCN'),
        func.expr('collect_set(MEAS_NR5G_PCC_PCI)').alias('NR5G_PCC_PCI'),
        func.expr('round(avg(MEAS_NR5G_PCC_RSRP_AVG), 2)').alias('NR5G_PCC_RSRP_Avg'),
        func.expr('round(min(MEAS_NR5G_PCC_RSRP_AVG), 2)').alias('NR5G_PCC_RSRP_Min'),
        func.expr('round(avg(MEAS_NR5G_PCC_SNR_AVG), 2)').alias('NR5G_PCC_SINR_Avg'),
        func.expr('round(min(MEAS_NR5G_PCC_SNR_AVG), 2)').alias('NR5G_PCC_SINR_Min'), ) \
        .sort(out_MEAS_NR.Failed_Event_Number.asc())
    # Code for \\\ Analysis Conditions ///
    out_MEAS_NR = out_MEAS_NR.withColumn("M_Analysis",
                                         when(out_MEAS_NR.NR5G_PCC_RSRP_Avg < -110, "Poor RF condition")
                                         .when(out_MEAS_NR.NR5G_PCC_SINR_Avg < 0, "Poor RF condition")
                                         .when(out_MEAS_NR.NR5G_PCC_RSRP_Avg.isNull()
                                               & out_MEAS_NR.NR5G_PCC_SINR_Avg.isNull(), 'Lost UE')
                                         .otherwise(None))
    out_MEAS_NR = out_MEAS_NR \
        .withColumn('NR5G_PCC_NARFCN', concat_ws(", ", col('NR5G_PCC_NARFCN'))) \
        .withColumn('NR5G_PCC_PCI', concat_ws(", ", col('NR5G_PCC_PCI')))
    return out_MEAS_NR


def f_signalling(df_Signalling):
    """
    This function return these tags
    1- (when Signalling table have no values in the time analysis window then 'Missing Signalling')
    2- (when column RRC_NR5G_event contains 'T-Mobile|AT&T' then 'UE on Roaming')

    :param df_Signalling:
    :return: out_Signalling_NR
    """
    an_w = an_window(df_event_NR)
    Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
    out_Signalling_NR = an_w.join(df_Signalling, Condition_Signalling)
    out_Signalling_NR = out_Signalling_NR.drop('Time', 'Failed_Event_Type', 'Event_Analysis_Start_Time',
                                               'Event_Analysis_End_Time')
    out_MissingSignalling = an_w.join(out_Signalling_NR, ['Failed_Event_Number'], 'left_anti')
    out_MissingSignalling = out_MissingSignalling.drop('Failed_Event_Type', 'Event_Analysis_Start_Time',
                                                       'Event_Analysis_End_Time')
    out_MissingSignalling = out_MissingSignalling.withColumn('MsgSig_Analysis', lit('Missing Signalling'))
    out_Signalling_NR = out_Signalling_NR.withColumn('MCC_MNC',
                                                     when(out_Signalling_NR.RRC_NR5G_event.contains('MCC :'),
                                                          (func.substring('RRC_NR5G_event', 35, 37)))
                                                     .otherwise(None))
    out_Signalling_NR = out_Signalling_NR.withColumn('S1_Analysis',
                                                     when(out_Signalling_NR.RRC_NR5G_event.rlike('T-Mobile|AT&T'),
                                                          'UE on Roaming')
                                                     .otherwise(None))
    out_Signalling_NR = out_Signalling_NR.groupby('Failed_Event_Number') \
        .agg(collect_set(out_Signalling_NR.MCC_MNC).alias('MCC_MNC'),
             collect_set(out_Signalling_NR.S1_Analysis).alias('S1_Analysis'), )
    out_Signalling_NR = out_Signalling_NR \
        .withColumn('MCC_MNC', concat_ws(", ", col('MCC_MNC'))) \
        .withColumn('S1_Analysis', concat_ws(", ", col('S1_Analysis')))
    out_Signalling_NR = out_Signalling_NR.withColumn('MCC_MNC', when(out_Signalling_NR.MCC_MNC == '', None).otherwise(
        out_Signalling_NR.MCC_MNC))
    out_Signalling_NR = out_Signalling_NR.join(out_MissingSignalling, ['Failed_Event_Number'], 'full')
    out_Signalling_NR = out_Signalling_NR.withColumn('S_Analysis', concat_ws(', ', out_Signalling_NR.S1_Analysis,
                                                                             out_Signalling_NR.MsgSig_Analysis))
    out_Signalling_NR = out_Signalling_NR.drop('S1_Analysis', 'MsgSig_Analysis')
    out_Signalling_NR = out_Signalling_NR.withColumn('S_Analysis',
                                                     when(out_Signalling_NR.S_Analysis == '', None).otherwise(
                                                         out_Signalling_NR.S_Analysis))
    return out_Signalling_NR


def f_nbr_an(df_Signalling, df_MEAS_NR):
    """
    This function return these tags
    1- (on the basis of measurementReport then rrcReconfiguration then
     systemInformationBlockType1 then rrcReconfigurationComplete and also Event A3 analysis 'Handover failure')

    :param df_Signalling:
    :param df_MEAS_NR:
    :return out_MissingNBR:
    """
    an_w = an_window(df_event_NR)
    # Code for \\\ NBR check ///
    Condition_Signalling2 = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
    out_MissingNBR = an_w.join(df_Signalling, Condition_Signalling2, 'left')
    out_MissingNBR = out_MissingNBR.drop('F_Event_CallEvent', 'Event_Analysis_Start_Time', 'Failed_Event_Type')
    out_MissingNBR = out_MissingNBR.sort(out_MissingNBR.Time.asc())
    out_MissingNBR = out_MissingNBR.withColumn('NBR_Check_0',
                                               when((out_MissingNBR.RRC_NR5G_event == '5GNR measurementReport')
                                                    & (out_MissingNBR.RRC_NR5G_message.contains(
                                                   'measResultNeighCells')), 'Check1')
                                               .when((out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfiguration')
                                                     & (out_MissingNBR.RRC_NR5G_message.contains(
                                                   'reconfigurationWithSync')), 'Check2')
                                               .otherwise(None))
    # Code for \\\ HO Failure ///
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    out_MissingNBR_T = out_MissingNBR.filter((out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfiguration') |
                                             (out_MissingNBR.RRC_NR5G_event.rlike('systemInformationBlockType1')) |
                                             (out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfigurationComplete'))
    schemaNB = func.schema_of_json(
        "{\"DL-DCCH-Message\":{\"message\":{\"c1\":{\"rrcReconfiguration\":{\"criticalExtensions\":{\"rrcReconfiguration\":{\"measConfig\":{\"measGapConfig\":{\"ext1\":{\"gapUE\":{\"release\":null}}},\"measIdToAddModList\":[{\"MeasIdToAddMod\":{\"measId\":2,\"measObjectId\":1,\"reportConfigId\":1}},{\"MeasIdToAddMod\":{\"measId\":3,\"measObjectId\":1,\"reportConfigId\":2}}],\"measIdToRemoveList\":[{\"MeasId\":1},{\"MeasId\":3},{\"MeasId\":8},{\"MeasId\":9}],\"measObjectToAddModList\":[{\"MeasObjectToAddMod\":{\"measObject\":{\"measObjectNR\":{\"offsetMO\":{\"rsrpOffsetCSI-RS\":\"dB0\",\"rsrpOffsetSSB\":\"dB0\",\"rsrqOffsetCSI-RS\":\"dB0\",\"rsrqOffsetSSB\":\"dB0\",\"sinrOffsetCSI-RS\":\"dB0\",\"sinrOffsetSSB\":\"dB0\"},\"quantityConfigIndex\":1,\"referenceSignalConfig\":{\"ssb-ConfigMobility\":{\"deriveSSB-IndexFromCell\":true,\"ssb-ToMeasure\":{\"setup\":{\"shortBitmap\":\"1101\"}}}},\"smtc1\":{\"duration\":\"sf5\",\"periodicityAndOffset\":{\"sf20\":0}},\"ssbFrequency\":129870,\"ssbSubcarrierSpacing\":\"kHz15\"}},\"measObjectId\":1}}],\"measObjectToRemoveList\":[{\"MeasObjectId\":1},{\"MeasObjectId\":2},{\"MeasObjectId\":3},{\"MeasObjectId\":4}],\"quantityConfig\":{\"quantityConfigNR-List\":[{\"QuantityConfigNR\":{\"quantityConfigCell\":{\"csi-RS-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"},\"ssb-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"}}}}]},\"reportConfigToAddModList\":[{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA2\":{\"a2-Threshold\":{\"rsrp\":61},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms40\"}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"r4\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":true,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":1}},{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA3\":{\"a3-Offset\":{\"rsrp\":6},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms640\",\"useAllowedCellList\":false}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"infinity\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":false,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":2}}],\"reportConfigToRemoveList\":[{\"ReportConfigId\":2},{\"ReportConfigId\":4},{\"ReportConfigId\":5}]},\"nonCriticalExtension\":{\"masterCellGroup\":{\"CellGroupConfig\":{\"cellGroupId\":0,\"mac-CellGroupConfig\":{\"bsr-Config\":{\"periodicBSR-Timer\":\"sf10\",\"retxBSR-Timer\":\"sf80\"},\"drx-Config\":{\"setup\":{\"drx-HARQ-RTT-TimerDL\":56,\"drx-HARQ-RTT-TimerUL\":56,\"drx-InactivityTimer\":\"ms100\",\"drx-LongCycleStartOffset\":{\"ms160\":9},\"drx-RetransmissionTimerDL\":\"sl8\",\"drx-RetransmissionTimerUL\":\"sl8\",\"drx-SlotOffset\":0,\"drx-onDurationTimer\":{\"milliSeconds\":\"ms10\"}}},\"ext1\":{\"csi-Mask\":false},\"phr-Config\":{\"setup\":{\"dummy\":false,\"multiplePHR\":false,\"phr-ModeOtherCG\":\"real\",\"phr-PeriodicTimer\":\"sf20\",\"phr-ProhibitTimer\":\"sf20\",\"phr-Tx-PowerFactorChange\":\"dB1\",\"phr-Type2OtherCell\":false}},\"schedulingRequestConfig\":{\"schedulingRequestToAddModList\":[{\"SchedulingRequestToAddMod\":{\"schedulingRequestId\":0,\"sr-ProhibitTimer\":\"ms1\",\"sr-TransMax\":\"n64\"}}]},\"skipUplinkTxDynamic\":false,\"tag-Config\":{\"tag-ToAddModList\":[{\"TAG\":{\"tag-Id\":0,\"timeAlignmentTimer\":\"infinity\"}}]}},\"physicalCellGroupConfig\":{\"ext2\":{},\"p-NR-FR1\":23,\"pdsch-HARQ-ACK-Codebook\":\"dynamic\"},\"rlc-BearerToAddModList\":[{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":1,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":1,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":2,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":3,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":4,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":2,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"kBps8\",\"priority\":8,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size18\",\"t-Reassembly\":\"ms45\",\"t-StatusProhibit\":\"ms20\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"p16\",\"sn-FieldLength\":\"size18\",\"t-PollRetransmit\":\"ms45\"}}}}}],\"spCellConfig\":{\"reconfigurationWithSync\":{\"newUE-Identity\":18298,\"spCellConfigCommon\":{\"dmrs-TypeA-Position\":\"pos2\",\"downlinkConfigCommon\":{\"frequencyInfoDL\":{\"absoluteFrequencyPointA\":129450,\"absoluteFrequencySSB\":129870,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialDownlinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pdcch-ConfigCommon\":{\"setup\":{\"commonSearchSpaceList\":[{\"SearchSpace\":{\"controlResourceSetId\":0,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n0\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n0\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n1\"},\"searchSpaceId\":1,\"searchSpaceType\":{\"common\":{\"dci-Format0-0-AndFormat1-0\":{}}}}}],\"controlResourceSetZero\":0,\"pagingSearchSpace\":0,\"ra-SearchSpace\":1,\"searchSpaceOtherSystemInformation\":1,\"searchSpaceSIB1\":0,\"searchSpaceZero\":0}},\"pdsch-ConfigCommon\":{\"setup\":{\"pdsch-TimeDomainAllocationList\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}}]}}}},\"n-TimingAdvanceOffset\":\"n0\",\"physCellId\":708,\"ss-PBCH-BlockPower\":27,\"ssb-PositionsInBurst\":{\"shortBitmap\":\"0100\"},\"ssb-periodicityServingCell\":\"ms20\",\"ssbSubcarrierSpacing\":\"kHz15\",\"uplinkConfigCommon\":{\"dummy\":\"infinity\",\"frequencyInfoUL\":{\"absoluteFrequencyPointA\":138650,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"p-Max\":23,\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialUplinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pucch-ConfigCommon\":{\"setup\":{\"p0-nominal\":-100,\"pucch-GroupHopping\":\"neither\",\"pucch-ResourceCommon\":0}},\"pusch-ConfigCommon\":{\"setup\":{\"msg3-DeltaPreamble\":0,\"p0-NominalWithGrant\":-98,\"pusch-TimeDomainAllocationList\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":55}}]}},\"rach-ConfigCommon\":{\"setup\":{\"prach-RootSequenceIndex\":{\"l839\":450},\"ra-ContentionResolutionTimer\":\"sf40\",\"rach-ConfigGeneric\":{\"msg1-FDM\":\"one\",\"msg1-FrequencyStart\":15,\"powerRampingStep\":\"dB2\",\"prach-ConfigurationIndex\":16,\"preambleReceivedTargetPower\":-106,\"preambleTransMax\":\"n10\",\"ra-ResponseWindow\":\"sl20\",\"zeroCorrelationZoneConfig\":12},\"restrictedSetConfig\":\"unrestrictedSet\",\"rsrp-ThresholdSSB\":0,\"ssb-perRACH-OccasionAndCB-PreamblesPerSSB\":{\"oneHalf\":\"n64\"}}}}}},\"t304\":\"ms2000\"},\"rlf-TimersAndConstants\":{\"setup\":{\"ext1\":{\"t311\":\"ms3000\"},\"n310\":\"n20\",\"n311\":\"n1\",\"t310\":\"ms2000\"}},\"servCellIndex\":0,\"spCellConfigDedicated\":{\"csi-MeasConfig\":{\"setup\":{\"csi-IM-ResourceSetToAddModList\":[{\"CSI-IM-ResourceSet\":{\"csi-IM-ResourceSetId\":0,\"csi-IM-Resources\":[{\"CSI-IM-ResourceId\":0}]}}],\"csi-IM-ResourceToAddModList\":[{\"CSI-IM-Resource\":{\"csi-IM-ResourceElementPattern\":{\"pattern1\":{\"subcarrierLocation-p1\":\"s0\",\"symbolLocation-p1\":13}},\"csi-IM-ResourceId\":0,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"periodicityAndOffset\":{\"slots40\":0}}}],\"csi-ReportConfigToAddModList\":[{\"CSI-ReportConfig\":{\"carrier\":0,\"codebookConfig\":{\"codebookType\":{\"type1\":{\"codebookMode\":1,\"subType\":{\"typeI-SinglePanel\":{\"nrOfAntennaPorts\":{\"moreThanTwo\":{\"n1-n2\":{\"two-one-TypeI-SinglePanel-Restriction\":\"11111111\"},\"typeI-SinglePanel-codebookSubsetRestriction-i2\":\"11111111 11111111\"}},\"typeI-SinglePanel-ri-Restriction\":\"00001111\"}}}}},\"cqi-Table\":\"table2\",\"csi-IM-ResourcesForInterference\":1,\"ext1\":{},\"groupBasedBeamReporting\":{\"disabled\":{\"nrofReportedRS\":\"n1\"}},\"reportConfigId\":0,\"reportConfigType\":{\"periodic\":{\"pucch-CSI-ResourceList\":[{\"PUCCH-CSI-Resource\":{\"pucch-Resource\":6,\"uplinkBandwidthPartId\":0}}],\"reportSlotConfig\":{\"slots160\":11}}},\"reportFreqConfiguration\":{\"cqi-FormatIndicator\":\"widebandCQI\",\"pmi-FormatIndicator\":\"widebandPMI\"},\"reportQuantity\":{\"cri-RI-PMI-CQI\":null},\"resourcesForChannelMeasurement\":0,\"subbandSize\":\"value1\",\"timeRestrictionForChannelMeasurements\":\"notConfigured\",\"timeRestrictionForInterferenceMeasurements\":\"notConfigured\"}}],\"csi-ResourceConfigToAddModList\":[{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"nzp-CSI-RS-SSB\":{\"nzp-CSI-RS-ResourceSetList\":[{\"NZP-CSI-RS-ResourceSetId\":0}]}},\"csi-ResourceConfigId\":0,\"resourceType\":\"periodic\"}},{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"csi-IM-ResourceSetList\":[{\"CSI-IM-ResourceSetId\":0}]},\"csi-ResourceConfigId\":1,\"resourceType\":\"periodic\"}}],\"nzp-CSI-RS-ResourceSetToAddModList\":[{\"NZP-CSI-RS-ResourceSet\":{\"nzp-CSI-RS-Resources\":[{\"NZP-CSI-RS-ResourceId\":0}],\"nzp-CSI-ResourceSetId\":0}}],\"nzp-CSI-RS-ResourceToAddModList\":[{\"NZP-CSI-RS-Resource\":{\"nzp-CSI-RS-ResourceId\":0,\"periodicityAndOffset\":{\"slots10\":0},\"powerControlOffset\":0,\"powerControlOffsetSS\":\"db0\",\"resourceMapping\":{\"cdm-Type\":\"fd-CDM2\",\"density\":{\"one\":null},\"firstOFDMSymbolInTimeDomain\":13,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"frequencyDomainAllocation\":{\"row4\":\"010\"},\"nrofPorts\":\"p4\"},\"scramblingID\":708}}]}},\"defaultDownlinkBWP-Id\":0,\"firstActiveDownlinkBWP-Id\":0,\"initialDownlinkBWP\":{\"pdcch-Config\":{\"setup\":{\"controlResourceSetToAddModList\":[{\"ControlResourceSet\":{\"cce-REG-MappingType\":{\"nonInterleaved\":null},\"controlResourceSetId\":1,\"duration\":2,\"frequencyDomainResources\":\"11110000 00000000 00000000 00000000 00000000 00000\",\"pdcch-DMRS-ScramblingID\":708,\"precoderGranularity\":\"sameAsREG-bundle\"}}],\"searchSpacesToAddModList\":[{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n2\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":2,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-1-And-1-1\"}}}},{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n4\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":3,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-0-And-1-0\"}}}}]}},\"pdsch-Config\":{\"setup\":{\"dmrs-DownlinkForPDSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\"}},\"maxNrofCodeWordsScheduledByDCI\":\"n1\",\"mcs-Table\":\"qam256\",\"pdsch-TimeDomainAllocationList\":{\"setup\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}},{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":67}}]},\"prb-BundlingType\":{\"staticBundling\":{}},\"rbg-Size\":\"config1\",\"resourceAllocation\":\"resourceAllocationType1\",\"tci-StatesToAddModList\":[{\"TCI-State\":{\"qcl-Type1\":{\"cell\":0,\"qcl-Type\":\"typeA\",\"referenceSignal\":{\"ssb\":1}},\"tci-StateId\":0}}]}}},\"pdsch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":2},\"nrofHARQ-ProcessesForPDSCH\":\"n16\"}},\"tag-Id\":0,\"uplinkConfig\":{\"firstActiveUplinkBWP-Id\":0,\"initialUplinkBWP\":{\"pucch-Config\":{\"setup\":{\"dl-DataToUL-ACK\":[{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4}],\"format2\":{\"setup\":{\"maxCodeRate\":\"zeroDot15\",\"simultaneousHARQ-ACK-CSI\":\"true\"}},\"pucch-PowerControl\":{\"deltaF-PUCCH-f0\":0,\"deltaF-PUCCH-f1\":0,\"deltaF-PUCCH-f2\":2,\"deltaF-PUCCH-f3\":0,\"p0-Set\":[{\"P0-PUCCH\":{\"p0-PUCCH-Id\":1,\"p0-PUCCH-Value\":0}}],\"pathlossReferenceRSs\":[{\"PUCCH-PathlossReferenceRS\":{\"pucch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"resourceSetToAddModList\":[{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":0,\"resourceList\":[{\"PUCCH-ResourceId\":1},{\"PUCCH-ResourceId\":2},{\"PUCCH-ResourceId\":3},{\"PUCCH-ResourceId\":4}]}},{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":1,\"resourceList\":[{\"PUCCH-ResourceId\":5}]}}],\"resourceToAddModList\":[{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":4,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":0,\"startingPRB\":4}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":1,\"startingPRB\":11}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":2,\"startingPRB\":12}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":3,\"startingPRB\":13}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":4,\"startingPRB\":14}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":9,\"nrofSymbols\":1,\"startingSymbolIndex\":12}},\"pucch-ResourceId\":5,\"startingPRB\":4}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":5,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":6,\"startingPRB\":6}}],\"schedulingRequestResourceToAddModList\":[{\"SchedulingRequestResourceConfig\":{\"periodicityAndOffset\":{\"sl10\":0},\"resource\":0,\"schedulingRequestID\":0,\"schedulingRequestResourceId\":1}}]}},\"pusch-Config\":{\"setup\":{\"codebookSubset\":\"nonCoherent\",\"dataScramblingIdentityPUSCH\":708,\"dmrs-UplinkForPUSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\",\"transformPrecodingDisabled\":{\"scramblingID0\":708}}},\"maxRank\":1,\"pusch-PowerControl\":{\"msg3-Alpha\":\"alpha1\",\"p0-AlphaSets\":[{\"P0-PUSCH-AlphaSet\":{\"alpha\":\"alpha1\",\"p0\":0,\"p0-PUSCH-AlphaSetId\":0}}],\"pathlossReferenceRSToAddModList\":[{\"PUSCH-PathlossReferenceRS\":{\"pusch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"pusch-TimeDomainAllocationList\":{\"setup\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":69}}]},\"resourceAllocation\":\"resourceAllocationType1\",\"transformPrecoder\":\"disabled\",\"txConfig\":\"codebook\"}},\"srs-Config\":{\"setup\":{\"srs-ResourceSetToAddModList\":[{\"SRS-ResourceSet\":{\"alpha\":\"alpha1\",\"p0\":-86,\"pathlossReferenceRS\":{\"ssb-Index\":1},\"resourceType\":{\"aperiodic\":{\"aperiodicSRS-ResourceTrigger\":1,\"csi-RS\":1,\"slotOffset\":1}},\"srs-PowerControlAdjustmentStates\":\"separateClosedLoop\",\"srs-ResourceIdList\":[{\"SRS-ResourceId\":0}],\"srs-ResourceSetId\":0,\"usage\":\"codebook\"}}],\"srs-ResourceToAddModList\":[{\"SRS-Resource\":{\"freqDomainPosition\":0,\"freqDomainShift\":0,\"freqHopping\":{\"b-SRS\":0,\"b-hop\":0,\"c-SRS\":0},\"groupOrSequenceHopping\":\"neither\",\"nrofSRS-Ports\":\"port1\",\"resourceMapping\":{\"nrofSymbols\":\"n1\",\"repetitionFactor\":\"n1\",\"startPosition\":0},\"resourceType\":{\"aperiodic\":{}},\"sequenceId\":0,\"spatialRelationInfo\":{\"referenceSignal\":{\"ssb-Index\":1},\"servingCellId\":0},\"srs-ResourceId\":0,\"transmissionComb\":{\"n4\":{\"combOffset-n4\":0,\"cyclicShift-n4\":0}}}}]}}},\"pusch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":1}}}}}}}}},\"radioBearerConfig\":{\"drb-ToAddModList\":[{\"DRB-ToAddMod\":{\"cnAssociation\":{\"sdap-Config\":{\"defaultDRB\":true,\"mappedQoS-FlowsToAdd\":[{\"QFI\":1}],\"pdu-Session\":2,\"sdap-HeaderDL\":\"present\",\"sdap-HeaderUL\":\"present\"}},\"drb-Identity\":1,\"pdcp-Config\":{\"drb\":{\"discardTimer\":\"infinity\",\"headerCompression\":{\"notUsed\":null},\"pdcp-SN-SizeDL\":\"len18bits\",\"pdcp-SN-SizeUL\":\"len18bits\",\"statusReportRequired\":\"true\"},\"t-Reordering\":\"ms140\"},\"recoverPDCP\":\"true\"}}],\"srb-ToAddModList\":[{\"SRB-ToAddMod\":{\"srb-Identity\":1}},{\"SRB-ToAddMod\":{\"srb-Identity\":2}}]}}},\"rrc-TransactionIdentifier\":2}}}}}\n")
    out_MissingNBR_T = out_MissingNBR_T.withColumn("jsonDataNB", func.from_json(col("RRC_NR5G_message"), schemaNB))
    out_MissingNBR_T = out_MissingNBR_T.select('Event_Analysis_End_Time', 'Failed_Event_Number', 'Time', 'WorkOrderId',
                                               'Device', 'RRC_NR5G_event', 'RRC_NR5G_pci', 'NBR_Check_0',
                                               col('jsonDataNB.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.nonCriticalExtension.masterCellGroup.CellGroupConfig.spCellConfig.reconfigurationWithSync.spCellConfigCommon.physCellId').alias(
                                                   'Target_PCI'),
                                               'RRC_NR5G_message')
    subWindow0 = Window.partitionBy('Failed_Event_Number').orderBy('Time')
    out_MissingNBR_T = out_MissingNBR_T.withColumn('TPCI1', func.lead('RRC_NR5G_pci', 1).over(subWindow0)) \
        .withColumn('TPCI2', func.lead('RRC_NR5G_pci', 2).over(subWindow0))
    out_MissingNBR_T = out_MissingNBR_T.withColumn('Check_TPCI',
                                                   when((col('Target_PCI') == col('TPCI1')) & (
                                                               col('Target_PCI') == col('TPCI2')),
                                                        'PCI_Check_OK').otherwise(None))
    out_MissingNBR_T = out_MissingNBR_T.select('Failed_Event_Number', 'Check_TPCI').filter(
        col('Check_TPCI') == 'PCI_Check_OK')
    out_MissingNBR = out_MissingNBR.join(out_MissingNBR_T, ['Failed_Event_Number'], 'left_anti')
    out_MissingNBR = out_MissingNBR.filter((out_MissingNBR.RRC_NR5G_event == '5GNR measurementReport') |
                                           (out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfiguration'))
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Code for \\\ EventA3 ///
    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    schemaUL = func.schema_of_json(
        "{\"UL-DCCH-Message\":{\"message\":{\"c1\":{\"measurementReport\":{\"criticalExtensions\":{\"measurementReport\":{\"measResults\":{\"measId\":3,\"measResultNeighCells\":{\"measResultListNR\":[{\"MeasResultNR\":{\"measResult\":{\"cellResults\":{\"resultsSSB-Cell\":{\"rsrp\":70}}},\"physCellId\":40}}]},\"measResultServingMOList\":[{\"MeasResultServMO\":{\"measResultServingCell\":{\"measResult\":{\"cellResults\":{\"resultsSSB-Cell\":{\"rsrp\":62,\"rsrq\":60}}},\"physCellId\":611},\"servCellId\":0}},{\"MeasResultServMO\":{\"measResultServingCell\":{\"measResult\":{\"cellResults\":{\"resultsSSB-Cell\":{\"rsrp\":50,\"rsrq\":65}}},\"physCellId\":611},\"servCellId\":2}}]}}}}}}}}")
    schemaDL = func.schema_of_json(
        "{\"DL-DCCH-Message\":{\"message\":{\"c1\":{\"rrcReconfiguration\":{\"criticalExtensions\":{\"rrcReconfiguration\":{\"measConfig\":{\"measIdToAddModList\":[{\"MeasIdToAddMod\":{\"measId\":2,\"measObjectId\":1,\"reportConfigId\":1}},{\"MeasIdToAddMod\":{\"measId\":3,\"measObjectId\":1,\"reportConfigId\":2}}],\"measIdToRemoveList\":[{\"MeasId\":1},{\"MeasId\":3}],\"measObjectToAddModList\":[{\"MeasObjectToAddMod\":{\"measObject\":{\"measObjectNR\":{\"offsetMO\":{\"rsrpOffsetCSI-RS\":\"dB0\",\"rsrpOffsetSSB\":\"dB0\",\"rsrqOffsetCSI-RS\":\"dB0\",\"rsrqOffsetSSB\":\"dB0\",\"sinrOffsetCSI-RS\":\"dB0\",\"sinrOffsetSSB\":\"dB0\"},\"quantityConfigIndex\":1,\"referenceSignalConfig\":{\"ssb-ConfigMobility\":{\"deriveSSB-IndexFromCell\":true,\"ssb-ToMeasure\":{\"setup\":{\"shortBitmap\":\"1101\"}}}},\"smtc1\":{\"duration\":\"sf5\",\"periodicityAndOffset\":{\"sf20\":0}},\"ssbFrequency\":129870,\"ssbSubcarrierSpacing\":\"kHz15\"}},\"measObjectId\":1}}],\"measObjectToRemoveList\":[{\"MeasObjectId\":1}],\"quantityConfig\":{\"quantityConfigNR-List\":[{\"QuantityConfigNR\":{\"quantityConfigCell\":{\"csi-RS-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"},\"ssb-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"}}}}]},\"reportConfigToAddModList\":[{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA2\":{\"a2-Threshold\":{\"rsrp\":61},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms40\"}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"r4\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":true,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":1}},{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA3\":{\"a3-Offset\":{\"rsrp\":6},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms640\",\"useAllowedCellList\":false}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"infinity\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":false,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":2}}],\"reportConfigToRemoveList\":[{\"ReportConfigId\":2},{\"ReportConfigId\":3},{\"ReportConfigId\":4}]},\"nonCriticalExtension\":{\"masterCellGroup\":{\"CellGroupConfig\":{\"cellGroupId\":0,\"mac-CellGroupConfig\":{\"bsr-Config\":{\"periodicBSR-Timer\":\"sf10\",\"retxBSR-Timer\":\"sf80\"},\"drx-Config\":{\"setup\":{\"drx-HARQ-RTT-TimerDL\":56,\"drx-HARQ-RTT-TimerUL\":56,\"drx-InactivityTimer\":\"ms100\",\"drx-LongCycleStartOffset\":{\"ms160\":9},\"drx-RetransmissionTimerDL\":\"sl8\",\"drx-RetransmissionTimerUL\":\"sl8\",\"drx-SlotOffset\":0,\"drx-onDurationTimer\":{\"milliSeconds\":\"ms10\"}}},\"ext1\":{\"csi-Mask\":false},\"phr-Config\":{\"setup\":{\"dummy\":false,\"multiplePHR\":false,\"phr-ModeOtherCG\":\"real\",\"phr-PeriodicTimer\":\"sf20\",\"phr-ProhibitTimer\":\"sf20\",\"phr-Tx-PowerFactorChange\":\"dB1\",\"phr-Type2OtherCell\":false}},\"schedulingRequestConfig\":{\"schedulingRequestToAddModList\":[{\"SchedulingRequestToAddMod\":{\"schedulingRequestId\":0,\"sr-ProhibitTimer\":\"ms1\",\"sr-TransMax\":\"n64\"}}]},\"skipUplinkTxDynamic\":false,\"tag-Config\":{\"tag-ToAddModList\":[{\"TAG\":{\"tag-Id\":0,\"timeAlignmentTimer\":\"infinity\"}}]}},\"physicalCellGroupConfig\":{\"ext2\":{},\"p-NR-FR1\":23,\"pdsch-HARQ-ACK-Codebook\":\"dynamic\"},\"rlc-BearerToAddModList\":[{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":1,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":1,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":2,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":3,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":4,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":3,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"kBps8\",\"priority\":10,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size18\",\"t-Reassembly\":\"ms45\",\"t-StatusProhibit\":\"ms20\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"p16\",\"sn-FieldLength\":\"size18\",\"t-PollRetransmit\":\"ms45\"}}}}}],\"spCellConfig\":{\"reconfigurationWithSync\":{\"newUE-Identity\":17292,\"spCellConfigCommon\":{\"dmrs-TypeA-Position\":\"pos2\",\"downlinkConfigCommon\":{\"frequencyInfoDL\":{\"absoluteFrequencyPointA\":129450,\"absoluteFrequencySSB\":129870,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialDownlinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pdcch-ConfigCommon\":{\"setup\":{\"commonSearchSpaceList\":[{\"SearchSpace\":{\"controlResourceSetId\":0,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n0\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n0\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n1\"},\"searchSpaceId\":1,\"searchSpaceType\":{\"common\":{\"dci-Format0-0-AndFormat1-0\":{}}}}}],\"controlResourceSetZero\":0,\"pagingSearchSpace\":0,\"ra-SearchSpace\":1,\"searchSpaceOtherSystemInformation\":1,\"searchSpaceSIB1\":0,\"searchSpaceZero\":2}},\"pdsch-ConfigCommon\":{\"setup\":{\"pdsch-TimeDomainAllocationList\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}}]}}}},\"n-TimingAdvanceOffset\":\"n0\",\"physCellId\":877,\"ss-PBCH-BlockPower\":27,\"ssb-PositionsInBurst\":{\"shortBitmap\":\"1000\"},\"ssb-periodicityServingCell\":\"ms20\",\"ssbSubcarrierSpacing\":\"kHz15\",\"uplinkConfigCommon\":{\"dummy\":\"infinity\",\"frequencyInfoUL\":{\"absoluteFrequencyPointA\":138650,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"p-Max\":23,\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialUplinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pucch-ConfigCommon\":{\"setup\":{\"p0-nominal\":-100,\"pucch-GroupHopping\":\"neither\",\"pucch-ResourceCommon\":0}},\"pusch-ConfigCommon\":{\"setup\":{\"msg3-DeltaPreamble\":0,\"p0-NominalWithGrant\":-98,\"pusch-TimeDomainAllocationList\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":55}}]}},\"rach-ConfigCommon\":{\"setup\":{\"prach-RootSequenceIndex\":{\"l839\":270},\"ra-ContentionResolutionTimer\":\"sf40\",\"rach-ConfigGeneric\":{\"msg1-FDM\":\"one\",\"msg1-FrequencyStart\":15,\"powerRampingStep\":\"dB2\",\"prach-ConfigurationIndex\":16,\"preambleReceivedTargetPower\":-106,\"preambleTransMax\":\"n10\",\"ra-ResponseWindow\":\"sl20\",\"zeroCorrelationZoneConfig\":12},\"restrictedSetConfig\":\"unrestrictedSet\",\"rsrp-ThresholdSSB\":0,\"ssb-perRACH-OccasionAndCB-PreamblesPerSSB\":{\"oneHalf\":\"n64\"}}}}}},\"t304\":\"ms2000\"},\"rlf-TimersAndConstants\":{\"setup\":{\"ext1\":{\"t311\":\"ms3000\"},\"n310\":\"n20\",\"n311\":\"n1\",\"t310\":\"ms2000\"}},\"servCellIndex\":0,\"spCellConfigDedicated\":{\"csi-MeasConfig\":{\"setup\":{\"csi-IM-ResourceSetToAddModList\":[{\"CSI-IM-ResourceSet\":{\"csi-IM-ResourceSetId\":0,\"csi-IM-Resources\":[{\"CSI-IM-ResourceId\":0}]}}],\"csi-IM-ResourceToAddModList\":[{\"CSI-IM-Resource\":{\"csi-IM-ResourceElementPattern\":{\"pattern1\":{\"subcarrierLocation-p1\":\"s0\",\"symbolLocation-p1\":13}},\"csi-IM-ResourceId\":0,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"periodicityAndOffset\":{\"slots40\":0}}}],\"csi-ReportConfigToAddModList\":[{\"CSI-ReportConfig\":{\"carrier\":0,\"codebookConfig\":{\"codebookType\":{\"type1\":{\"codebookMode\":1,\"subType\":{\"typeI-SinglePanel\":{\"nrOfAntennaPorts\":{\"moreThanTwo\":{\"n1-n2\":{\"two-one-TypeI-SinglePanel-Restriction\":\"11111111\"},\"typeI-SinglePanel-codebookSubsetRestriction-i2\":\"11111111 11111111\"}},\"typeI-SinglePanel-ri-Restriction\":\"00001111\"}}}}},\"cqi-Table\":\"table2\",\"csi-IM-ResourcesForInterference\":1,\"ext1\":{},\"groupBasedBeamReporting\":{\"disabled\":{\"nrofReportedRS\":\"n1\"}},\"reportConfigId\":0,\"reportConfigType\":{\"periodic\":{\"pucch-CSI-ResourceList\":[{\"PUCCH-CSI-Resource\":{\"pucch-Resource\":6,\"uplinkBandwidthPartId\":0}}],\"reportSlotConfig\":{\"slots160\":11}}},\"reportFreqConfiguration\":{\"cqi-FormatIndicator\":\"widebandCQI\",\"pmi-FormatIndicator\":\"widebandPMI\"},\"reportQuantity\":{\"cri-RI-PMI-CQI\":null},\"resourcesForChannelMeasurement\":0,\"subbandSize\":\"value1\",\"timeRestrictionForChannelMeasurements\":\"notConfigured\",\"timeRestrictionForInterferenceMeasurements\":\"notConfigured\"}}],\"csi-ResourceConfigToAddModList\":[{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"nzp-CSI-RS-SSB\":{\"nzp-CSI-RS-ResourceSetList\":[{\"NZP-CSI-RS-ResourceSetId\":0}]}},\"csi-ResourceConfigId\":0,\"resourceType\":\"periodic\"}},{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"csi-IM-ResourceSetList\":[{\"CSI-IM-ResourceSetId\":0}]},\"csi-ResourceConfigId\":1,\"resourceType\":\"periodic\"}}],\"nzp-CSI-RS-ResourceSetToAddModList\":[{\"NZP-CSI-RS-ResourceSet\":{\"nzp-CSI-RS-Resources\":[{\"NZP-CSI-RS-ResourceId\":0}],\"nzp-CSI-ResourceSetId\":0}}],\"nzp-CSI-RS-ResourceToAddModList\":[{\"NZP-CSI-RS-Resource\":{\"nzp-CSI-RS-ResourceId\":0,\"periodicityAndOffset\":{\"slots10\":0},\"powerControlOffset\":0,\"powerControlOffsetSS\":\"db0\",\"resourceMapping\":{\"cdm-Type\":\"fd-CDM2\",\"density\":{\"one\":null},\"firstOFDMSymbolInTimeDomain\":13,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"frequencyDomainAllocation\":{\"row4\":\"010\"},\"nrofPorts\":\"p4\"},\"scramblingID\":877}}]}},\"defaultDownlinkBWP-Id\":0,\"firstActiveDownlinkBWP-Id\":0,\"initialDownlinkBWP\":{\"pdcch-Config\":{\"setup\":{\"controlResourceSetToAddModList\":[{\"ControlResourceSet\":{\"cce-REG-MappingType\":{\"nonInterleaved\":null},\"controlResourceSetId\":1,\"duration\":2,\"frequencyDomainResources\":\"11110000 00000000 00000000 00000000 00000000 00000\",\"pdcch-DMRS-ScramblingID\":877,\"precoderGranularity\":\"sameAsREG-bundle\"}}],\"searchSpacesToAddModList\":[{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n2\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":2,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-1-And-1-1\"}}}},{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n4\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":3,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-0-And-1-0\"}}}}]}},\"pdsch-Config\":{\"setup\":{\"dmrs-DownlinkForPDSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\"}},\"maxNrofCodeWordsScheduledByDCI\":\"n1\",\"mcs-Table\":\"qam256\",\"pdsch-TimeDomainAllocationList\":{\"setup\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}},{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":67}}]},\"prb-BundlingType\":{\"staticBundling\":{}},\"rbg-Size\":\"config1\",\"resourceAllocation\":\"resourceAllocationType1\",\"tci-StatesToAddModList\":[{\"TCI-State\":{\"qcl-Type1\":{\"cell\":0,\"qcl-Type\":\"typeA\",\"referenceSignal\":{\"ssb\":0}},\"tci-StateId\":0}}]}}},\"pdsch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":2},\"nrofHARQ-ProcessesForPDSCH\":\"n16\"}},\"tag-Id\":0,\"uplinkConfig\":{\"firstActiveUplinkBWP-Id\":0,\"initialUplinkBWP\":{\"pucch-Config\":{\"setup\":{\"dl-DataToUL-ACK\":[{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4}],\"format2\":{\"setup\":{\"maxCodeRate\":\"zeroDot15\",\"simultaneousHARQ-ACK-CSI\":\"true\"}},\"pucch-PowerControl\":{\"deltaF-PUCCH-f0\":0,\"deltaF-PUCCH-f1\":0,\"deltaF-PUCCH-f2\":2,\"deltaF-PUCCH-f3\":0,\"p0-Set\":[{\"P0-PUCCH\":{\"p0-PUCCH-Id\":1,\"p0-PUCCH-Value\":0}}],\"pathlossReferenceRSs\":[{\"PUCCH-PathlossReferenceRS\":{\"pucch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"resourceSetToAddModList\":[{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":0,\"resourceList\":[{\"PUCCH-ResourceId\":1},{\"PUCCH-ResourceId\":2},{\"PUCCH-ResourceId\":3},{\"PUCCH-ResourceId\":4}]}},{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":1,\"resourceList\":[{\"PUCCH-ResourceId\":5}]}}],\"resourceToAddModList\":[{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":2,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":0,\"startingPRB\":5}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":1,\"startingPRB\":11}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":2,\"startingPRB\":12}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":3,\"startingPRB\":13}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":4,\"startingPRB\":14}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":9,\"nrofSymbols\":1,\"startingSymbolIndex\":12}},\"pucch-ResourceId\":5,\"startingPRB\":4}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":5,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":6,\"startingPRB\":6}}],\"schedulingRequestResourceToAddModList\":[{\"SchedulingRequestResourceConfig\":{\"periodicityAndOffset\":{\"sl10\":4},\"resource\":0,\"schedulingRequestID\":0,\"schedulingRequestResourceId\":1}}]}},\"pusch-Config\":{\"setup\":{\"codebookSubset\":\"nonCoherent\",\"dataScramblingIdentityPUSCH\":877,\"dmrs-UplinkForPUSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\",\"transformPrecodingDisabled\":{\"scramblingID0\":877}}},\"maxRank\":1,\"pusch-PowerControl\":{\"msg3-Alpha\":\"alpha1\",\"p0-AlphaSets\":[{\"P0-PUSCH-AlphaSet\":{\"alpha\":\"alpha1\",\"p0\":0,\"p0-PUSCH-AlphaSetId\":0}}],\"pathlossReferenceRSToAddModList\":[{\"PUSCH-PathlossReferenceRS\":{\"pusch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"pusch-TimeDomainAllocationList\":{\"setup\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":55}},{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":55}}]},\"resourceAllocation\":\"resourceAllocationType1\",\"transformPrecoder\":\"disabled\",\"txConfig\":\"codebook\"}},\"srs-Config\":{\"setup\":{\"srs-ResourceSetToAddModList\":[{\"SRS-ResourceSet\":{\"alpha\":\"alpha1\",\"p0\":-86,\"pathlossReferenceRS\":{\"ssb-Index\":0},\"resourceType\":{\"aperiodic\":{\"aperiodicSRS-ResourceTrigger\":1,\"csi-RS\":0,\"slotOffset\":1}},\"srs-PowerControlAdjustmentStates\":\"separateClosedLoop\",\"srs-ResourceIdList\":[{\"SRS-ResourceId\":0}],\"srs-ResourceSetId\":0,\"usage\":\"codebook\"}}],\"srs-ResourceToAddModList\":[{\"SRS-Resource\":{\"freqDomainPosition\":0,\"freqDomainShift\":0,\"freqHopping\":{\"b-SRS\":0,\"b-hop\":0,\"c-SRS\":0},\"groupOrSequenceHopping\":\"neither\",\"nrofSRS-Ports\":\"port1\",\"resourceMapping\":{\"nrofSymbols\":\"n1\",\"repetitionFactor\":\"n1\",\"startPosition\":0},\"resourceType\":{\"aperiodic\":{}},\"sequenceId\":0,\"spatialRelationInfo\":{\"referenceSignal\":{\"ssb-Index\":0},\"servingCellId\":0},\"srs-ResourceId\":0,\"transmissionComb\":{\"n4\":{\"combOffset-n4\":0,\"cyclicShift-n4\":0}}}}]}}},\"pusch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":1}}}}}}}}},\"radioBearerConfig\":{\"drb-ToAddModList\":[{\"DRB-ToAddMod\":{\"cnAssociation\":{\"sdap-Config\":{\"defaultDRB\":true,\"mappedQoS-FlowsToAdd\":[{\"QFI\":1}],\"pdu-Session\":2,\"sdap-HeaderDL\":\"present\",\"sdap-HeaderUL\":\"present\"}},\"drb-Identity\":1,\"pdcp-Config\":{\"drb\":{\"discardTimer\":\"infinity\",\"headerCompression\":{\"notUsed\":null},\"pdcp-SN-SizeDL\":\"len18bits\",\"pdcp-SN-SizeUL\":\"len18bits\",\"statusReportRequired\":\"true\"},\"t-Reordering\":\"ms140\"},\"recoverPDCP\":\"true\"}}],\"srb-ToAddModList\":[{\"SRB-ToAddMod\":{\"srb-Identity\":1}},{\"SRB-ToAddMod\":{\"srb-Identity\":2}}]}}},\"rrc-TransactionIdentifier\":1}}}}}")
    out_MissingNBR = out_MissingNBR.withColumn("jsonDataUL",
                                               func.from_json(col("RRC_NR5G_message"), schemaUL)).withColumn(
        "jsonDataDL", func.from_json(col("RRC_NR5G_message"), schemaDL))
    out_MissingNBR = out_MissingNBR.select('Event_Analysis_End_Time', 'Failed_Event_Number', 'Time', 'WorkOrderId',
                                           'Device', 'RRC_NR5G_event', 'NBR_Check_0',
                                           col('jsonDataUL.UL-DCCH-Message.message.c1.measurementReport.criticalExtensions.measurementReport.measResults.measId').alias(
                                               'measId_MR'),
                                           col('jsonDataDL.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.measIdToAddModList.MeasIdToAddMod.measId').alias(
                                               'measId_RC'),
                                           col('jsonDataDL.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.measIdToAddModList.MeasIdToAddMod.reportConfigId').alias(
                                               'reportConfigId_m'),
                                           col('jsonDataDL.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.reportConfigToAddModList.ReportConfigToAddMod.reportConfigId').alias(
                                               'reportConfigId_evt'),
                                           col('jsonDataDL.DL-DCCH-Message.message.c1.rrcReconfiguration.criticalExtensions.rrcReconfiguration.measConfig.reportConfigToAddModList.ReportConfigToAddMod.reportConfig.reportConfigNR.reportType.eventTriggered.eventId.eventA3.a3-Offset.rsrp')[
                                               1].alias('EventA3'),
                                           'RRC_NR5G_message')
    out_MissingNBR = out_MissingNBR.withColumn('NBR_Check', concat_ws(", ", col('NBR_Check_0'), col('EventA3')))
    out_MissingNBR = out_MissingNBR.drop('NBR_Check_0', 'measId_MR', 'measId_RC', 'reportConfigId_m',
                                         'reportConfigId_evt', 'EventA3')
    out_MissingNBR = out_MissingNBR.sort(out_MissingNBR.Time.asc())

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
    # Code for \\\ SubWindow ///
    out_subWindow = out_MissingNBR.filter(out_MissingNBR.NBR_Check == 'Check1')
    subWindow1 = Window.partitionBy('Failed_Event_Number').orderBy('Time').rowsBetween(Window.unboundedPreceding,
                                                                                       Window.unboundedFollowing)
    out_subWindow = out_subWindow.withColumn('SWnd_StartTime', func.last('Time').over(subWindow1))
    out_subWindow = out_subWindow.select('Failed_Event_Number', 'SWnd_StartTime', 'Event_Analysis_End_Time').distinct()

    # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

    Condition_MEAS_NR2 = (df_MEAS_NR["Time"] >= out_subWindow["SWnd_StartTime"]) & (
                df_MEAS_NR["Time"] <= out_subWindow["Event_Analysis_End_Time"])
    out_MEAS_NR_sub = out_subWindow.join(df_MEAS_NR, Condition_MEAS_NR2, 'leftouter').sort(df_MEAS_NR.Time.asc())
    out_MEAS_NR_sub = out_MEAS_NR_sub.drop('Failed_Event_Type')
    out_MEAS_NR_sub = out_MEAS_NR_sub.groupby('Failed_Event_Number').agg(
        func.expr('collect_set(MEAS_NR5G_PCC_PCI)').alias('NR5G_PCC_PCI_Last'),
        func.expr('round(avg(MEAS_NR5G_PCC_RSRP_AVG), 2)').alias('NR5G_PCC_RSRP_Last'),
        func.expr('round(avg(MEAS_NR5G_PCC_SNR_AVG), 2)').alias('NR5G_PCC_SINR_Last'), ) \
        .sort(out_MEAS_NR_sub.Failed_Event_Number.asc())
    out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('NR5G_PCC_PCI_Last', concat_ws(", ", col('NR5G_PCC_PCI_Last')))
    out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('Length_abc', length(out_MEAS_NR_sub.NR5G_PCC_PCI_Last))
    out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('Length_abc2', when(out_MEAS_NR_sub.Length_abc < 5, 'NCheck1')
                                                 .otherwise(None))
    out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('RadioCondition',
                                                 when(out_MEAS_NR_sub.NR5G_PCC_RSRP_Last < -110, 'NCheck2')
                                                 .when(out_MEAS_NR_sub.NR5G_PCC_SINR_Last < 0, 'NCheck2')
                                                 .otherwise(None))
    out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('SubWCheck', concat_ws(', ', out_MEAS_NR_sub.Length_abc2,
                                                                        out_MEAS_NR_sub.RadioCondition))
    out_MEAS_NR_sub = out_MEAS_NR_sub.drop('Length_abc', 'Length_abc2', 'RadioCondition')
    out_MissingNBR = out_MissingNBR.groupby('Failed_Event_Number').agg(
        collect_set(out_MissingNBR.NBR_Check).alias('NBR_Check'), )
    out_MissingNBR = out_MissingNBR.withColumn('NBR_Check', concat_ws(', ', col('NBR_Check')))
    out_MissingNBR = out_MissingNBR.join(out_MEAS_NR_sub, ['Failed_Event_Number'], 'left')
    out_MissingNBR = out_MissingNBR.withColumn('NBR_CheckAll',
                                               concat_ws(', ', out_MissingNBR.NBR_Check, out_MissingNBR.SubWCheck))
    out_MissingNBR = out_MissingNBR.withColumn('N_Analysis',
                                               when(out_MissingNBR.NBR_CheckAll == 'Check1, NCheck1, NCheck2',
                                                    'Handover failure')
                                               .otherwise(None))
    out_MissingNBR = out_MissingNBR.drop('NBR_Check', 'SubWCheck', 'NBR_CheckAll')
    return out_MissingNBR


def f_rach_fail(df_RACH_NR):
    """
    This function return these tags
    1- (when RACHT_NR5G_RACHReason == 0 then
    RACHA_NR5G_RACHResult == 0 then rach failure where only RACHReason = 0 'RACH failure')

    :param df_RACH_NR:
    :return out_RACH_NR:
    """
    an_w = an_window(df_event_NR)
    Condition_RACH_NR = (df_RACH_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_RACH_NR["Time"] <= an_w["Event_Analysis_End_Time"])
    out_RACH_NR = an_w.join(df_RACH_NR, Condition_RACH_NR)
    out_RACH_NR = out_RACH_NR.drop('F_Event_CallEvent', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time', 'Failed_Event_Type', 'WorkOrderId', 'Device')
    out_RACH_NR = out_RACH_NR.sort(out_RACH_NR.Time.asc())
    out_RACH_NR = out_RACH_NR.withColumn('RaCheck1',
                                         when(out_RACH_NR.RACHT_NR5G_RACHReason == 0, 'RCheck1').otherwise(None)) \
        .withColumn('RaCheck2', when(out_RACH_NR.RACHA_NR5G_RACHResult == 0, 'RCheck2').otherwise(None))
    out_RACH_NR = out_RACH_NR.withColumn('RaCheck', concat_ws(', ', col('RaCheck1'), col('RaCheck2')))
    out_RACH_NR = out_RACH_NR.groupby('Failed_Event_Number').agg(collect_set(out_RACH_NR.RaCheck).alias('RaCheck'), )
    out_RACH_NR = out_RACH_NR.withColumn('RaCheck', concat_ws(', ', col('RaCheck')))
    out_RACH_NR = out_RACH_NR.filter(out_RACH_NR.RaCheck == 'RCheck1, ').withColumn('RaAnalysis', lit('RACH failure'))
    out_RACH_NR = out_RACH_NR.drop('RaCheck')
    return out_RACH_NR


def f_ping_pong_ho(df_Signalling):
    """
    This function return these tags
    1- (when time between two Handovers is <= 5s then find three handovers having time <=5
    then tag as 'Ping_Pong Handover')

    :param out_Signalling:
    :return: out_Signalling
    """

    an_w = an_window(df_event_NR)
    Condition_png = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
    out_Signalling = an_w.join(df_Signalling, Condition_png)
    out_Signalling = out_Signalling.drop('Event_Analysis_Start_Time', 'Event_Analysis_End_Time', 'Failed_Event_Type')
    out_Signalling = out_Signalling.sort(df_Signalling.Time.asc())
    cndfilter = ['5GNR rrcReconfiguration', '5GNR rrcReconfigurationComplete']
    out_Signalling = out_Signalling.filter((out_Signalling.RRC_NR5G_event.isin(cndfilter)) | (out_Signalling.RRC_NR5G_event.rlike('systemInformationBlockType1')))
    out_Signalling = out_Signalling.withColumn('NBR_Check_0',
                                             when((out_Signalling.RRC_NR5G_event == '5GNR rrcReconfiguration') & (
                                                 out_Signalling.RRC_NR5G_message.contains('reconfigurationWithSync')),
                                                  'HO_Com_sent')
                                             .when(out_Signalling.RRC_NR5G_event == '5GNR rrcReconfigurationComplete',
                                                   'HO_Com_Rcv')
                                             .when(out_Signalling.RRC_NR5G_event.rlike('systemInformationBlockType1'),
                                                   'HO_Com_cplt')
                                             .otherwise(None))
    out_Signalling = out_Signalling.filter(out_Signalling.NBR_Check_0.isNotNull())
    out_Signalling = out_Signalling.drop('RRC_NR5G_event', 'RRC_NR5G_message')

    subWindow0 = Window.partitionBy('Device').orderBy('Time')
    out_Signalling = out_Signalling\
        .withColumn('Rcv', func.lead('NBR_Check_0', 1).over(subWindow0))\
        .withColumn('cplt', func.lead('NBR_Check_0', 2).over(subWindow0))\
        .withColumn('S_PCI', out_Signalling.RRC_NR5G_pci)\
        .withColumn('T_PCI', func.lead('RRC_NR5G_pci', 2).over(subWindow0))
    out_Signalling = out_Signalling\
        .withColumn('Complete_Cmd', concat_ws('|', col('NBR_Check_0'), col('Rcv'), col('cplt')))\
        .withColumn('HO1_PCIs', concat_ws('|', col('S_PCI'), col('T_PCI')))
    out_Signalling = out_Signalling.drop('RRC_NR5G_pci', 'NBR_Check_0', 'Rcv', 'cplt', 'S_PCI', 'T_PCI')
    out_Signalling = out_Signalling.filter(out_Signalling.Complete_Cmd == 'HO_Com_sent|HO_Com_Rcv|HO_Com_cplt')
    out_Signalling = out_Signalling.drop('Complete_Cmd')
    subWindow1 = Window.partitionBy('Device').orderBy('Time')
    out_Signalling = out_Signalling\
        .withColumn('HO2_Time', func.lead('Time', 1).over(subWindow1))\
        .withColumn('HO2_PCIs', func.lead('HO1_PCIs', 1).over(subWindow1))
    out_Signalling = out_Signalling.withColumn('Time_Delta_sec', col('HO2_Time').cast('double') - col('Time').cast('double'))
    out_Signalling = out_Signalling.withColumn('cck', when(out_Signalling.Time_Delta_sec <= 5, 'CC').otherwise(None))
    out_Signalling = out_Signalling.withColumn("next_cck", func.lead("cck").over(Window.partitionBy('Device').orderBy("Time")))
    out_Signalling = out_Signalling.withColumn('PngPong_Analysis', when(col('cck') == col('next_cck'), 'Ping_Pong Handover').otherwise(None))
    out_Signalling = out_Signalling.filter(col('PngPong_Analysis') == 'Ping_Pong Handover')
    out_Signalling = out_Signalling.drop('Time', 'WorkOrderId', 'Device', 'HO1_PCIs', 'HO2_Time', 'HO2_PCIs', 'Time_Delta_sec', 'cck', 'next_cck')

    return out_Signalling


def f_reestab_fail(df_Signalling):
    """
    This function return this tag 'rrcReestablishment Failure'
    1- (when rrcReestablishmentRequest and (rrcSetup or rrcReestablishment)
    and (rrcReestablishmentComplete or rrcSetupComplete) are successful then
    these are rrcReestablishment successful otherwise fail and this function
    return only failures 'rrcReestablishment Failure')

    :param df_Signalling:
    :return out_reestab_fail:
    """
    an_w = an_window(df_event_NR)
    Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
    out_reestab_fail = an_w.join(df_Signalling, Condition_Signalling, 'left')
    out_reestab_fail = out_reestab_fail.drop('Time', 'Failed_Event_Type', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time',
                                             'WorkOrderId', 'Device', 'RRC_NR5G_pci', 'RRC_NR5G_message')
    out_reestab_fail = out_reestab_fail.withColumn('r_f_Check1', when(
        out_reestab_fail.RRC_NR5G_event == '5GNR rrcReestablishmentRequest', 'rfc_1').otherwise(None))
    out_reestab_fail = out_reestab_fail.withColumn('r_f_Check2', when(
        (out_reestab_fail.RRC_NR5G_event == '5GNR rrcSetup') |
        (out_reestab_fail.RRC_NR5G_event == '5GNR rrcReestablishment'), 'rfc_2').otherwise(None))
    out_reestab_fail = out_reestab_fail.withColumn('r_f_Check3', when(
        (out_reestab_fail.RRC_NR5G_event == '5GNR rrcReestablishmentComplete') |
        (out_reestab_fail.RRC_NR5G_event.rlike('rrcSetupComplete')), 'rfc_3').otherwise(None))
    out_reestab_fail = out_reestab_fail.withColumn('r_f_Check3_f', concat_ws(',', col('r_f_Check1'), col('r_f_Check2'), col('r_f_Check3')))
    out_reestab_fail = out_reestab_fail.filter(col('r_f_Check3_f') != '')
    out_reestab_fail = out_reestab_fail.groupby('Failed_Event_Number').agg(collect_set(out_reestab_fail.r_f_Check3_f).alias('r_f_Check3_f'), )
    out_reestab_fail = out_reestab_fail.withColumn('r_f_Check3_f', concat_ws("|", col('r_f_Check3_f')))
    out_reestab_fail = out_reestab_fail.filter(out_reestab_fail.r_f_Check3_f.isin(['rfc_1', 'rfc_1|rfc_2']))\
        .withColumn('ResFAnalysis', lit('rrcReestablishment Failure'))
    out_reestab_fail = out_reestab_fail.drop('r_f_Check3_f')

    return out_reestab_fail


def f_reestablishment(df_Signalling):
    """
    This function return this tag 'rrcReestablishment'
    1- (when rrcReestablishmentRequest and (rrcSetup or rrcReestablishment)
    and (rrcReestablishmentComplete or rrcSetupComplete) are successful then
    these are rrcReestablishment successful otherwise fail and this function
    return only successful 'rrcReestablishment')

    :param df_Signalling:
    :return: out_reestablishment
    """
    an_w = an_window(df_event_NR)
    Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
    out_reestablishment = an_w.join(df_Signalling, Condition_Signalling, 'left')
    out_reestablishment = out_reestablishment.drop('Time', 'Failed_Event_Type', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time',
                                             'WorkOrderId', 'Device', 'RRC_NR5G_pci', 'RRC_NR5G_message')
    out_reestablishment = out_reestablishment.withColumn('r_f_Check1', when(
        out_reestablishment.RRC_NR5G_event == '5GNR rrcReestablishmentRequest', 'rfc_1').otherwise(None))
    out_reestablishment = out_reestablishment.withColumn('r_f_Check2', when(
        (out_reestablishment.RRC_NR5G_event == '5GNR rrcSetup') |
        (out_reestablishment.RRC_NR5G_event == '5GNR rrcReestablishment'), 'rfc_2').otherwise(None))
    out_reestablishment = out_reestablishment.withColumn('r_f_Check3', when(
        (out_reestablishment.RRC_NR5G_event == '5GNR rrcReestablishmentComplete') |
        (out_reestablishment.RRC_NR5G_event.rlike('rrcSetupComplete')), 'rfc_3').otherwise(None))
    out_reestablishment = out_reestablishment.withColumn('r_f_Check3_f', concat_ws('|', col('r_f_Check1'), col('r_f_Check2'), col('r_f_Check3')))
    out_reestablishment = out_reestablishment.filter(col('r_f_Check3_f') != '')
    out_reestablishment = out_reestablishment.groupby('Failed_Event_Number').agg(collect_set(out_reestablishment.r_f_Check3_f).alias('r_f_Check3_f'))
    out_reestablishment = out_reestablishment.withColumn('r_f_Check3_f', concat_ws("|", col('r_f_Check3_f')))
    out_reestablishment = out_reestablishment.filter(out_reestablishment.r_f_Check3_f == 'rfc_1|rfc_2|rfc_3')\
        .withColumn('ResmntAnalysis', lit('rrcReestablishment'))
    out_reestablishment = out_reestablishment.drop('r_f_Check3_f')

    return out_reestablishment


def f_ref_cols(df_rc):
    """
    This function return only reference columns from event_NR table

    :param df_rc:
    :return out_df_rc:
    """
    an_w = an_window(df_event_NR)
    out_df_rc = df_rc.join(an_w, (df_rc["Time"] == an_w["Event_Analysis_End_Time"])).sort(
        df_rc.Time.asc())
    out_df_rc = out_df_rc.drop('Failed_Event_Type', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time')
    return out_df_rc


def f_final_df():
    """
    This function join all the functions output and combine for
    final analysis column.

    :return out_FinalDF:
    """
    an_w = an_window(df_event_NR)
    out_FinalDF = an_w.join(f_ref_cols(df_rc), ['Failed_Event_Number'], 'full') \
        .join(f_meas_nr(df_MEAS_NR), ['Failed_Event_Number'], 'full') \
        .join(f_signalling(df_Signalling), ['Failed_Event_Number'], 'full') \
        .join(f_nbr_an(df_Signalling, df_MEAS_NR), ['Failed_Event_Number'], 'full') \
        .join(f_rach_fail(df_RACH_NR), ['Failed_Event_Number'], 'full')\
        .join(f_ping_pong_ho(df_Signalling), ['Failed_Event_Number'], 'full')\
        .join(f_reestab_fail(df_Signalling), ['Failed_Event_Number'], 'full')\
        .join(f_reestablishment(df_Signalling), ['Failed_Event_Number'], 'full')
    out_FinalDF = out_FinalDF.withColumn('Analysis',
                                         when((out_FinalDF.M_Analysis.isNull()
                                               & out_FinalDF.S_Analysis.isNull()
                                               & out_FinalDF.N_Analysis.isNull()
                                               & out_FinalDF.RaAnalysis.isNull()
                                               & out_FinalDF.PngPong_Analysis.isNull()
                                               & out_FinalDF.ResFAnalysis.isNull()
                                               & out_FinalDF.ResmntAnalysis.isNull()),
                                              'Unidentified')
                                         .otherwise(concat_ws(', ', out_FinalDF.M_Analysis,
                                                              out_FinalDF.S_Analysis,
                                                              out_FinalDF.N_Analysis,
                                                              out_FinalDF.RaAnalysis,
                                                              out_FinalDF.PngPong_Analysis,
                                                              out_FinalDF.ResFAnalysis,
                                                              out_FinalDF.ResmntAnalysis)))
    out_FinalDF = out_FinalDF.drop('M_Analysis',
                                   'S_Analysis',
                                   'N_Analysis',
                                   'RaAnalysis',
                                   'PngPong_Analysis',
                                   'ResFAnalysis',
                                   'ResmntAnalysis')
    return out_FinalDF


s_device = 'IPERF3_DL'
df_event_NR, df_MEAS_NR, df_Signalling, df_RACH_NR, df_rc = read_all_tables_remote(s_device)
o_final_df1 = f_final_df()

df_event_NR.unpersist()
df_MEAS_NR.unpersist()
df_Signalling.unpersist()
df_RACH_NR.unpersist()
df_rc.unpersist()

s_device = 'IPERF3_UL'
df_event_NR, df_MEAS_NR, df_Signalling, df_RACH_NR, df_rc = read_all_tables_remote(s_device)
o_final_df2 = f_final_df()

o_final_df = o_final_df1.unionAll(o_final_df2)
# o_final_df = o_final_df.select('Failed_Event_Number', 'Failed_Event_Type', 'WorkOrderId', 'Device', 'Analysis')
# o_final_df.show(truncate=False)
# o_final_df.write.mode('overwrite').options(header='True').csv('/home/spark-master/Public/Raheel/Out1')

pdFrame = o_final_df.toPandas()
pdFrame["Time"] = pdFrame["Time"].dt.strftime("%Y-%m-%d")
pdFrame["Event_Analysis_Start_Time"] = pdFrame["Event_Analysis_Start_Time"].dt.strftime("%Y-%m-%d")
pdFrame["Event_Analysis_End_Time"] = pdFrame["Event_Analysis_End_Time"].dt.strftime("%Y-%m-%d")
pdFrame = pdFrame.replace({np.nan: None})
com.output(pdFrame, "Data_Call_Traffic_Failure_Analysis", cols)
print("\n--- Query process time is %s minutes ---\n" % ((time.time() - start_time)/60))





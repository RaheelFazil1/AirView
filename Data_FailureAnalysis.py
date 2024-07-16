from commonFunctions import CommonFunctions
# from rcomm import CommonFunctions
from pyspark.sql import SparkSession
from pyspark.sql.functions import concat_ws, col, lit, lag, collect_set, length
from pyspark.sql.functions import when
from pyspark.sql.window import Window
from pyspark.sql.functions import monotonically_increasing_id
import pyspark.sql.functions as func
import numpy as np
import pandas as pd
#from pyspark.sql.types import StringType
import time
start_time = time.time()


com = CommonFunctions("/home/spark-master/Public/JDBC/postgresql-42.5.0.jar")
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
    {"columnName": "NR5G_PCC_RSRP_Last", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "NR5G_PCC_SINR_Last", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "PHYDL_NR5G_PCC_BLER_Avg", "dataTypeName": "Nullable(float8)", "isNullable": True},
    {"columnName": "Analysis", "dataTypeName": "Nullable(String)", "isNullable": True}

]


print('\nStarted!\n')


class DataFailure:
    def __init__(self, s_device):
        gdevice = com.getDevices(s_device)
        print('its gdevice: ', gdevice)
        gdevice = gdevice[0]
        print('its gdevice: ', gdevice)
        
        self.df_event_NR = com.readData("Event_NR", ['Time', 'WorkOrderId', 'Device', 'Event_CallEvent'], "Time", "asc")
        self.df_event_NR = self.df_event_NR.filter(self.df_event_NR.Device == gdevice)

        self.df_MEAS_NR = com.readData("MEAS_NR",
                                  ['Time', 'WorkOrderId', 'Device', 'MEAS_NR5G_PCC_NARFCN', 'MEAS_NR5G_PCC_PCI',
                                   'MEAS_NR5G_PCC_RSRP_AVG', 'MEAS_NR5G_PCC_SNR_AVG'], "Time", "asc")
        self.df_MEAS_NR = self.df_MEAS_NR.filter(self.df_MEAS_NR.Device == gdevice)

        self.df_Signalling = com.readData("Signalling", ['Time', 'WorkOrderId', 'Device', 'RRC_NR5G_event', 'RRC_NR5G_pci',
                                                    'RRC_NR5G_message'], "Time", "asc")
        self.df_Signalling = self.df_Signalling.filter(self.df_Signalling.Device == gdevice)

        self.df_RACH_NR = com.readData("RACH_NR",
                                  ['Time', 'WorkOrderId', 'Device', 'RACHT_NR5G_CRNTI', 'RACHT_NR5G_RACHReason',
                                   'RACHA_NR5G_numAttmpt', 'RACHA_NR5G_RACHResult'], "Time", "asc")
        self.df_RACH_NR = self.df_RACH_NR.filter(self.df_RACH_NR.Device == gdevice)

        self.df_rc = com.readData("Event_NR",
                             ['Time', 'WorkOrderId', 'NetworkModeId', 'BandId', 'CarrierId', 'SectorId', 'Device',
                              'DeviceId', 'FileId', 'DimensionColumnsId', 'Epoch', 'MsgId', 'Latitude', 'Longitude'],
                             "Time", "asc")
        self.df_rc = self.df_rc.filter(self.df_rc.Device == gdevice)

        self.df_PHYDL_NR = com.readData("PHYDL_NR", ['Time', 'WorkOrderId', 'Device', 'PHYDL_NR5G_PCC_bler'], "Time", "asc")
        self.df_PHYDL_NR = self.df_PHYDL_NR.filter(self.df_PHYDL_NR.Device == gdevice)

    def an_window(self):
        Condition_Window = (self.df_event_NR.Event_CallEvent == '"Setup Start"') | \
                           (self.df_event_NR.Event_CallEvent == '"Call End"') | (
                               self.df_event_NR.Event_CallEvent.contains('Traffic Fail'))
        df1 = self.df_event_NR.select('WorkOrderId', 'Time', 'Event_CallEvent').filter(Condition_Window).sort(
            self.df_event_NR.Time.asc())
        partition = Window.partitionBy('WorkOrderId').orderBy('Time')
        df1 = df1.withColumn("Event_Analysis_Start_Time", lag("Time", 1).over(partition))
        failswindow = df1.filter(df1.Event_CallEvent.contains('Traffic Fail'))
        failswindow = failswindow.withColumn("Failed_Event_Number", monotonically_increasing_id() + 1)
        failswindow = failswindow.select(failswindow.Event_CallEvent.alias('Failed_Event_Type'),
                                         'Event_Analysis_Start_Time', failswindow.Time.alias('Event_Analysis_End_Time'),
                                         'Failed_Event_Number')
        return failswindow

    def f_radio_cnd(self):
        """
        This function return these tags
        1- (when RSRP_Avg < -110, "Poor RF condition")
        2- (when SINR_Avg < 0, "Poor RF condition")
        3- (when RSRP_Avg.isNull() and SINR_Avg.isNull(), 'Lost UE')

        :param self.df_MEAS_NR:
        :return out_MEAS_NR:
        """
        df_MEAS_NR = self.df_MEAS_NR
        an_w = self.an_window()
        Condition_MEAS_NR = (df_MEAS_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_MEAS_NR["Time"] <= an_w["Event_Analysis_End_Time"])
        out_MEAS_NR = an_w.join(df_MEAS_NR, Condition_MEAS_NR, 'leftouter').sort(df_MEAS_NR.Time.asc())
        out_MEAS_NR = out_MEAS_NR.drop('Failed_Event_Type')

        # tag time code
        t_rsrp = out_MEAS_NR.select('Failed_Event_Number', 'Time', 'MEAS_NR5G_PCC_RSRP_AVG').sort('Time')\
            .filter(col('MEAS_NR5G_PCC_RSRP_AVG') < -110).dropDuplicates(['Failed_Event_Number'])
        t_rsrp = t_rsrp.select('Failed_Event_Number', 'Time', lit('Poor RF condition').alias('Tag_Name')).sort('Time')
        t_snr = out_MEAS_NR.select('Failed_Event_Number', 'Time', 'MEAS_NR5G_PCC_SNR_AVG').sort('Time') \
            .filter(col('MEAS_NR5G_PCC_SNR_AVG') < 0).dropDuplicates(['Failed_Event_Number'])
        t_snr = t_snr.select('Failed_Event_Number', 'Time', lit('Poor RF condition').alias('Tag_Name')).sort('Time')
        t_radio = t_rsrp.union(t_snr)
        t_radio = t_radio.sort('Time')
        t_radio = t_radio.dropDuplicates(['Failed_Event_Number'])
        t_radio = t_radio.sort('Time')


        out_MEAS_NR = out_MEAS_NR.groupby('Failed_Event_Number').agg(
            func.expr('collect_set(MEAS_NR5G_PCC_NARFCN)').alias('NR5G_PCC_NARFCN'),
            func.expr('collect_set(MEAS_NR5G_PCC_PCI)').alias('NR5G_PCC_PCI'),
            func.expr('round(avg(MEAS_NR5G_PCC_RSRP_AVG), 2)').alias('NR5G_PCC_RSRP_Avg'),
            func.expr('round(min(MEAS_NR5G_PCC_RSRP_AVG), 2)').alias('NR5G_PCC_RSRP_Min'),
            func.expr('round(avg(MEAS_NR5G_PCC_SNR_AVG), 2)').alias('NR5G_PCC_SINR_Avg'),
            func.expr('round(min(MEAS_NR5G_PCC_SNR_AVG), 2)').alias('NR5G_PCC_SINR_Min'), ) \
            .sort(out_MEAS_NR.Failed_Event_Number.asc())
        out_MEAS_NR = out_MEAS_NR.withColumn("radio_Analysis",
                                             when((out_MEAS_NR.NR5G_PCC_RSRP_Avg < -110) | (out_MEAS_NR.NR5G_PCC_SINR_Avg < 0), "Poor RF condition")
                                             .otherwise(None))
        out_MEAS_NR = out_MEAS_NR \
            .withColumn('NR5G_PCC_NARFCN', concat_ws(", ", col('NR5G_PCC_NARFCN'))) \
            .withColumn('NR5G_PCC_PCI', concat_ws(", ", col('NR5G_PCC_PCI')))

        out_MEAS_NR = out_MEAS_NR.filter(col('radio_Analysis').isNotNull())

        t_radio = t_radio.join(out_MEAS_NR, ['Failed_Event_Number'], 'leftsemi')

        return out_MEAS_NR, t_radio

    def f_lost_ue(self):
        """
        This function return these tags
        1- (when RSRP_Avg.isNull() and SINR_Avg.isNull(), 'Lost UE')

        :param self.df_MEAS_NR:
        :return out_MEAS_NR:
        """
        df_MEAS_NR = self.df_MEAS_NR
        an_w = self.an_window()
        Condition_MEAS_NR = (df_MEAS_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_MEAS_NR["Time"] <= an_w["Event_Analysis_End_Time"])
        out_MEAS_NR = an_w.join(df_MEAS_NR, Condition_MEAS_NR, 'leftouter').sort(df_MEAS_NR.Time.asc())
        out_MEAS_NR = out_MEAS_NR.drop('Failed_Event_Type')

        # tag time code
        t_lost = an_w.select('Failed_Event_Number', col('Event_Analysis_Start_Time').alias('Time'), lit('Lost UE').alias('Tag_Name'))


        out_MEAS_NR = out_MEAS_NR.groupby('Failed_Event_Number').agg(
            func.expr('round(avg(MEAS_NR5G_PCC_RSRP_AVG), 2)').alias('NR5G_PCC_RSRP_Avg'),
            func.expr('round(avg(MEAS_NR5G_PCC_SNR_AVG), 2)').alias('NR5G_PCC_SINR_Avg'), ) \
            .sort(out_MEAS_NR.Failed_Event_Number.asc())
        out_MEAS_NR = out_MEAS_NR.withColumn("lost_Analysis",
                                             when(out_MEAS_NR.NR5G_PCC_RSRP_Avg.isNull()
                                                  & out_MEAS_NR.NR5G_PCC_SINR_Avg.isNull(), 'Lost UE')
                                             .otherwise(None))
        out_MEAS_NR = out_MEAS_NR.filter(col('lost_Analysis').isNotNull())
        out_MEAS_NR = out_MEAS_NR.drop('NR5G_PCC_RSRP_Avg', 'NR5G_PCC_SINR_Avg')
        t_lost = t_lost.join(out_MEAS_NR, ['Failed_Event_Number'], 'leftsemi')

        return out_MEAS_NR, t_lost

    def f_msing_sig(self):
        """
        This function return these tags
        1- (when Signalling table have no values in the time analysis window then 'Missing Signalling')

        :param self.df_Signalling:
        :return: out_Signalling_NR
        """
        df_Signalling = self.df_Signalling
        an_w = self.an_window()
        Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
        out_Signalling_NR = an_w.join(df_Signalling, Condition_Signalling)
        out_Signalling_NR = out_Signalling_NR.drop('Failed_Event_Type', 'Event_Analysis_Start_Time',
                                                   'Event_Analysis_End_Time')
        out_MissingSignalling = an_w.join(out_Signalling_NR, ['Failed_Event_Number'], 'left_anti')

        # tag time code
        t_msr1 = out_MissingSignalling.select('Failed_Event_Number', col('Event_Analysis_Start_Time').alias('Time'),
                                          lit('Missing Signalling').alias('Tag_Name')).sort('Time')

        out_MissingSignalling = out_MissingSignalling.drop('Failed_Event_Type', 'Event_Analysis_Start_Time',
                                                           'Event_Analysis_End_Time')
        out_MissingSignalling = out_MissingSignalling.withColumn('MsgSig_Analysis', lit('Missing Signalling'))
        t_msr1 = t_msr1.join(out_MissingSignalling, ['Failed_Event_Number'], 'leftsemi')

        return out_MissingSignalling, t_msr1

    def f_raoming(self):
        """
        This function return these tags
        1- (when column RRC_NR5G_event contains 'T-Mobile|AT&T' then 'UE on Roaming')

        :param self.df_Signalling:
        :return: out_Signalling_NR
        """
        df_Signalling = self.df_Signalling
        an_w = self.an_window()
        Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
        out_Signalling_NR = an_w.join(df_Signalling, Condition_Signalling)
        out_Signalling_NR = out_Signalling_NR.drop('Failed_Event_Type', 'Event_Analysis_Start_Time',
                                                   'Event_Analysis_End_Time')
        out_Signalling_NR = out_Signalling_NR.withColumn('MCC_MNC',
                                                         when(out_Signalling_NR.RRC_NR5G_event.contains('MCC :'),
                                                              (func.substring('RRC_NR5G_event', 35, 37)))
                                                         .otherwise(None))
        out_Signalling_NR = out_Signalling_NR.withColumn('romg_Analysis',
                                                         when(out_Signalling_NR.RRC_NR5G_event.rlike('T-Mobile|AT&T'),
                                                              'UE on Roaming')
                                                         .otherwise(None))

        # tag time code
        t_msr2 = out_Signalling_NR.select('Failed_Event_Number', 'Time', 'romg_Analysis').sort('Time')
        t_msr2 = t_msr2.filter(col('romg_Analysis') == 'UE on Roaming')
        t_msr2 = t_msr2.select('Failed_Event_Number', 'Time', col('romg_Analysis').alias('Tag_Name'))
        t_msr2 = t_msr2.sort('Time')
        t_msr2 = t_msr2.dropDuplicates(['Failed_Event_Number'])
        t_msr2 = t_msr2.sort('Time')


        out_Signalling_NR = out_Signalling_NR.groupby('Failed_Event_Number') \
            .agg(collect_set(out_Signalling_NR.MCC_MNC).alias('MCC_MNC'),
                 collect_set(out_Signalling_NR.romg_Analysis).alias('romg_Analysis'), )
        out_Signalling_NR = out_Signalling_NR \
            .withColumn('MCC_MNC', concat_ws(", ", col('MCC_MNC'))) \
            .withColumn('romg_Analysis', concat_ws(", ", col('romg_Analysis')))
        out_Signalling_NR = out_Signalling_NR.withColumn('MCC_MNC',
                                                         when(out_Signalling_NR.MCC_MNC == '', None).otherwise(
                                                             out_Signalling_NR.MCC_MNC))
        out_Signalling_NR = out_Signalling_NR.filter(col('romg_Analysis') == 'UE on Roaming')
        t_msr2 = t_msr2.join(out_Signalling_NR, ['Failed_Event_Number'], 'leftsemi')

        return out_Signalling_NR, t_msr2

    def f_ho_failure(self):
        """
        This function return these tags
        1- (on the basis of measurementReport then rrcReconfiguration then
         systemInformationBlockType1 then rrcReconfigurationComplete and also Event A3 analysis 'Handover failure')

        :param self.df_Signalling:
        :param self.df_MEAS_NR:
        :return out_MissingNBR:
        """
        df_Signalling = self.df_Signalling
        df_MEAS_NR = self.df_MEAS_NR
        an_w = self.an_window()
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


        # tag time
        t_hof = out_MissingNBR.select('Failed_Event_Number', 'Time', 'NBR_Check_0')\
            .filter(col('NBR_Check_0') == 'Check1').sort('Time')
        t_hof = t_hof.sort('Time').dropDuplicates(['Failed_Event_Number'])
        t_hof = t_hof.select('Failed_Event_Number', 'Time', lit('Handover failure').alias('Tag_Name')).sort('Time')


        # Code for \\\ HO Failure ///
        # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        out_MissingNBR_T = out_MissingNBR.filter((out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfiguration') |
                                                 (out_MissingNBR.RRC_NR5G_event.rlike('systemInformationBlockType1')) |
                                                 (out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfigurationComplete'))
        schemaNB = func.schema_of_json(
            "{\"DL-DCCH-Message\":{\"message\":{\"c1\":{\"rrcReconfiguration\":{\"criticalExtensions\":{\"rrcReconfiguration\":{\"measConfig\":{\"measGapConfig\":{\"ext1\":{\"gapUE\":{\"release\":null}}},\"measIdToAddModList\":[{\"MeasIdToAddMod\":{\"measId\":2,\"measObjectId\":1,\"reportConfigId\":1}},{\"MeasIdToAddMod\":{\"measId\":3,\"measObjectId\":1,\"reportConfigId\":2}}],\"measIdToRemoveList\":[{\"MeasId\":1},{\"MeasId\":3},{\"MeasId\":8},{\"MeasId\":9}],\"measObjectToAddModList\":[{\"MeasObjectToAddMod\":{\"measObject\":{\"measObjectNR\":{\"offsetMO\":{\"rsrpOffsetCSI-RS\":\"dB0\",\"rsrpOffsetSSB\":\"dB0\",\"rsrqOffsetCSI-RS\":\"dB0\",\"rsrqOffsetSSB\":\"dB0\",\"sinrOffsetCSI-RS\":\"dB0\",\"sinrOffsetSSB\":\"dB0\"},\"quantityConfigIndex\":1,\"referenceSignalConfig\":{\"ssb-ConfigMobility\":{\"deriveSSB-IndexFromCell\":true,\"ssb-ToMeasure\":{\"setup\":{\"shortBitmap\":\"1101\"}}}},\"smtc1\":{\"duration\":\"sf5\",\"periodicityAndOffset\":{\"sf20\":0}},\"ssbFrequency\":129870,\"ssbSubcarrierSpacing\":\"kHz15\"}},\"measObjectId\":1}}],\"measObjectToRemoveList\":[{\"MeasObjectId\":1},{\"MeasObjectId\":2},{\"MeasObjectId\":3},{\"MeasObjectId\":4}],\"quantityConfig\":{\"quantityConfigNR-List\":[{\"QuantityConfigNR\":{\"quantityConfigCell\":{\"csi-RS-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"},\"ssb-FilterConfig\":{\"filterCoefficientRS-SINR\":\"fc8\",\"filterCoefficientRSRP\":\"fc8\",\"filterCoefficientRSRQ\":\"fc8\"}}}}]},\"reportConfigToAddModList\":[{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA2\":{\"a2-Threshold\":{\"rsrp\":61},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms40\"}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"r4\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":true,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":1}},{\"ReportConfigToAddMod\":{\"reportConfig\":{\"reportConfigNR\":{\"reportType\":{\"eventTriggered\":{\"eventId\":{\"eventA3\":{\"a3-Offset\":{\"rsrp\":6},\"hysteresis\":2,\"reportOnLeave\":false,\"timeToTrigger\":\"ms640\",\"useAllowedCellList\":false}},\"includeBeamMeasurements\":false,\"maxReportCells\":8,\"reportAmount\":\"infinity\",\"reportInterval\":\"ms240\",\"reportQuantityCell\":{\"rsrp\":true,\"rsrq\":false,\"sinr\":false},\"rsType\":\"ssb\"}}}},\"reportConfigId\":2}}],\"reportConfigToRemoveList\":[{\"ReportConfigId\":2},{\"ReportConfigId\":4},{\"ReportConfigId\":5}]},\"nonCriticalExtension\":{\"masterCellGroup\":{\"CellGroupConfig\":{\"cellGroupId\":0,\"mac-CellGroupConfig\":{\"bsr-Config\":{\"periodicBSR-Timer\":\"sf10\",\"retxBSR-Timer\":\"sf80\"},\"drx-Config\":{\"setup\":{\"drx-HARQ-RTT-TimerDL\":56,\"drx-HARQ-RTT-TimerUL\":56,\"drx-InactivityTimer\":\"ms100\",\"drx-LongCycleStartOffset\":{\"ms160\":9},\"drx-RetransmissionTimerDL\":\"sl8\",\"drx-RetransmissionTimerUL\":\"sl8\",\"drx-SlotOffset\":0,\"drx-onDurationTimer\":{\"milliSeconds\":\"ms10\"}}},\"ext1\":{\"csi-Mask\":false},\"phr-Config\":{\"setup\":{\"dummy\":false,\"multiplePHR\":false,\"phr-ModeOtherCG\":\"real\",\"phr-PeriodicTimer\":\"sf20\",\"phr-ProhibitTimer\":\"sf20\",\"phr-Tx-PowerFactorChange\":\"dB1\",\"phr-Type2OtherCell\":false}},\"schedulingRequestConfig\":{\"schedulingRequestToAddModList\":[{\"SchedulingRequestToAddMod\":{\"schedulingRequestId\":0,\"sr-ProhibitTimer\":\"ms1\",\"sr-TransMax\":\"n64\"}}]},\"skipUplinkTxDynamic\":false,\"tag-Config\":{\"tag-ToAddModList\":[{\"TAG\":{\"tag-Id\":0,\"timeAlignmentTimer\":\"infinity\"}}]}},\"physicalCellGroupConfig\":{\"ext2\":{},\"p-NR-FR1\":23,\"pdsch-HARQ-ACK-Codebook\":\"dynamic\"},\"rlc-BearerToAddModList\":[{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":1,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":1,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":2,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":0,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"infinity\",\"priority\":3,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size12\",\"t-Reassembly\":\"ms35\",\"t-StatusProhibit\":\"ms0\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"infinity\",\"sn-FieldLength\":\"size12\",\"t-PollRetransmit\":\"ms45\"}}}}},{\"RLC-BearerConfig\":{\"logicalChannelIdentity\":4,\"mac-LogicalChannelConfig\":{\"ul-SpecificParameters\":{\"bucketSizeDuration\":\"ms50\",\"logicalChannelGroup\":2,\"logicalChannelSR-DelayTimerApplied\":false,\"logicalChannelSR-Mask\":false,\"prioritisedBitRate\":\"kBps8\",\"priority\":8,\"schedulingRequestID\":0}},\"reestablishRLC\":\"true\",\"rlc-Config\":{\"am\":{\"dl-AM-RLC\":{\"sn-FieldLength\":\"size18\",\"t-Reassembly\":\"ms45\",\"t-StatusProhibit\":\"ms20\"},\"ul-AM-RLC\":{\"maxRetxThreshold\":\"t32\",\"pollByte\":\"infinity\",\"pollPDU\":\"p16\",\"sn-FieldLength\":\"size18\",\"t-PollRetransmit\":\"ms45\"}}}}}],\"spCellConfig\":{\"reconfigurationWithSync\":{\"newUE-Identity\":18298,\"spCellConfigCommon\":{\"dmrs-TypeA-Position\":\"pos2\",\"downlinkConfigCommon\":{\"frequencyInfoDL\":{\"absoluteFrequencyPointA\":129450,\"absoluteFrequencySSB\":129870,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialDownlinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pdcch-ConfigCommon\":{\"setup\":{\"commonSearchSpaceList\":[{\"SearchSpace\":{\"controlResourceSetId\":0,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n0\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n0\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n1\"},\"searchSpaceId\":1,\"searchSpaceType\":{\"common\":{\"dci-Format0-0-AndFormat1-0\":{}}}}}],\"controlResourceSetZero\":0,\"pagingSearchSpace\":0,\"ra-SearchSpace\":1,\"searchSpaceOtherSystemInformation\":1,\"searchSpaceSIB1\":0,\"searchSpaceZero\":0}},\"pdsch-ConfigCommon\":{\"setup\":{\"pdsch-TimeDomainAllocationList\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}}]}}}},\"n-TimingAdvanceOffset\":\"n0\",\"physCellId\":708,\"ss-PBCH-BlockPower\":27,\"ssb-PositionsInBurst\":{\"shortBitmap\":\"0100\"},\"ssb-periodicityServingCell\":\"ms20\",\"ssbSubcarrierSpacing\":\"kHz15\",\"uplinkConfigCommon\":{\"dummy\":\"infinity\",\"frequencyInfoUL\":{\"absoluteFrequencyPointA\":138650,\"frequencyBandList\":[{\"FreqBandIndicatorNR\":71}],\"p-Max\":23,\"scs-SpecificCarrierList\":[{\"SCS-SpecificCarrier\":{\"carrierBandwidth\":25,\"offsetToCarrier\":0,\"subcarrierSpacing\":\"kHz15\"}}]},\"initialUplinkBWP\":{\"genericParameters\":{\"locationAndBandwidth\":6600,\"subcarrierSpacing\":\"kHz15\"},\"pucch-ConfigCommon\":{\"setup\":{\"p0-nominal\":-100,\"pucch-GroupHopping\":\"neither\",\"pucch-ResourceCommon\":0}},\"pusch-ConfigCommon\":{\"setup\":{\"msg3-DeltaPreamble\":0,\"p0-NominalWithGrant\":-98,\"pusch-TimeDomainAllocationList\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":55}}]}},\"rach-ConfigCommon\":{\"setup\":{\"prach-RootSequenceIndex\":{\"l839\":450},\"ra-ContentionResolutionTimer\":\"sf40\",\"rach-ConfigGeneric\":{\"msg1-FDM\":\"one\",\"msg1-FrequencyStart\":15,\"powerRampingStep\":\"dB2\",\"prach-ConfigurationIndex\":16,\"preambleReceivedTargetPower\":-106,\"preambleTransMax\":\"n10\",\"ra-ResponseWindow\":\"sl20\",\"zeroCorrelationZoneConfig\":12},\"restrictedSetConfig\":\"unrestrictedSet\",\"rsrp-ThresholdSSB\":0,\"ssb-perRACH-OccasionAndCB-PreamblesPerSSB\":{\"oneHalf\":\"n64\"}}}}}},\"t304\":\"ms2000\"},\"rlf-TimersAndConstants\":{\"setup\":{\"ext1\":{\"t311\":\"ms3000\"},\"n310\":\"n20\",\"n311\":\"n1\",\"t310\":\"ms2000\"}},\"servCellIndex\":0,\"spCellConfigDedicated\":{\"csi-MeasConfig\":{\"setup\":{\"csi-IM-ResourceSetToAddModList\":[{\"CSI-IM-ResourceSet\":{\"csi-IM-ResourceSetId\":0,\"csi-IM-Resources\":[{\"CSI-IM-ResourceId\":0}]}}],\"csi-IM-ResourceToAddModList\":[{\"CSI-IM-Resource\":{\"csi-IM-ResourceElementPattern\":{\"pattern1\":{\"subcarrierLocation-p1\":\"s0\",\"symbolLocation-p1\":13}},\"csi-IM-ResourceId\":0,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"periodicityAndOffset\":{\"slots40\":0}}}],\"csi-ReportConfigToAddModList\":[{\"CSI-ReportConfig\":{\"carrier\":0,\"codebookConfig\":{\"codebookType\":{\"type1\":{\"codebookMode\":1,\"subType\":{\"typeI-SinglePanel\":{\"nrOfAntennaPorts\":{\"moreThanTwo\":{\"n1-n2\":{\"two-one-TypeI-SinglePanel-Restriction\":\"11111111\"},\"typeI-SinglePanel-codebookSubsetRestriction-i2\":\"11111111 11111111\"}},\"typeI-SinglePanel-ri-Restriction\":\"00001111\"}}}}},\"cqi-Table\":\"table2\",\"csi-IM-ResourcesForInterference\":1,\"ext1\":{},\"groupBasedBeamReporting\":{\"disabled\":{\"nrofReportedRS\":\"n1\"}},\"reportConfigId\":0,\"reportConfigType\":{\"periodic\":{\"pucch-CSI-ResourceList\":[{\"PUCCH-CSI-Resource\":{\"pucch-Resource\":6,\"uplinkBandwidthPartId\":0}}],\"reportSlotConfig\":{\"slots160\":11}}},\"reportFreqConfiguration\":{\"cqi-FormatIndicator\":\"widebandCQI\",\"pmi-FormatIndicator\":\"widebandPMI\"},\"reportQuantity\":{\"cri-RI-PMI-CQI\":null},\"resourcesForChannelMeasurement\":0,\"subbandSize\":\"value1\",\"timeRestrictionForChannelMeasurements\":\"notConfigured\",\"timeRestrictionForInterferenceMeasurements\":\"notConfigured\"}}],\"csi-ResourceConfigToAddModList\":[{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"nzp-CSI-RS-SSB\":{\"nzp-CSI-RS-ResourceSetList\":[{\"NZP-CSI-RS-ResourceSetId\":0}]}},\"csi-ResourceConfigId\":0,\"resourceType\":\"periodic\"}},{\"CSI-ResourceConfig\":{\"bwp-Id\":0,\"csi-RS-ResourceSetList\":{\"csi-IM-ResourceSetList\":[{\"CSI-IM-ResourceSetId\":0}]},\"csi-ResourceConfigId\":1,\"resourceType\":\"periodic\"}}],\"nzp-CSI-RS-ResourceSetToAddModList\":[{\"NZP-CSI-RS-ResourceSet\":{\"nzp-CSI-RS-Resources\":[{\"NZP-CSI-RS-ResourceId\":0}],\"nzp-CSI-ResourceSetId\":0}}],\"nzp-CSI-RS-ResourceToAddModList\":[{\"NZP-CSI-RS-Resource\":{\"nzp-CSI-RS-ResourceId\":0,\"periodicityAndOffset\":{\"slots10\":0},\"powerControlOffset\":0,\"powerControlOffsetSS\":\"db0\",\"resourceMapping\":{\"cdm-Type\":\"fd-CDM2\",\"density\":{\"one\":null},\"firstOFDMSymbolInTimeDomain\":13,\"freqBand\":{\"nrofRBs\":28,\"startingRB\":0},\"frequencyDomainAllocation\":{\"row4\":\"010\"},\"nrofPorts\":\"p4\"},\"scramblingID\":708}}]}},\"defaultDownlinkBWP-Id\":0,\"firstActiveDownlinkBWP-Id\":0,\"initialDownlinkBWP\":{\"pdcch-Config\":{\"setup\":{\"controlResourceSetToAddModList\":[{\"ControlResourceSet\":{\"cce-REG-MappingType\":{\"nonInterleaved\":null},\"controlResourceSetId\":1,\"duration\":2,\"frequencyDomainResources\":\"11110000 00000000 00000000 00000000 00000000 00000\",\"pdcch-DMRS-ScramblingID\":708,\"precoderGranularity\":\"sameAsREG-bundle\"}}],\"searchSpacesToAddModList\":[{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n2\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":2,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-1-And-1-1\"}}}},{\"SearchSpace\":{\"controlResourceSetId\":1,\"monitoringSlotPeriodicityAndOffset\":{\"sl1\":null},\"monitoringSymbolsWithinSlot\":\"10000000 000000\",\"nrofCandidates\":{\"aggregationLevel1\":\"n4\",\"aggregationLevel16\":\"n0\",\"aggregationLevel2\":\"n4\",\"aggregationLevel4\":\"n2\",\"aggregationLevel8\":\"n2\"},\"searchSpaceId\":3,\"searchSpaceType\":{\"ue-Specific\":{\"dci-Formats\":\"formats0-0-And-1-0\"}}}}]}},\"pdsch-Config\":{\"setup\":{\"dmrs-DownlinkForPDSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\"}},\"maxNrofCodeWordsScheduledByDCI\":\"n1\",\"mcs-Table\":\"qam256\",\"pdsch-TimeDomainAllocationList\":{\"setup\":[{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":53}},{\"PDSCH-TimeDomainResourceAllocation\":{\"k0\":0,\"mappingType\":\"typeA\",\"startSymbolAndLength\":67}}]},\"prb-BundlingType\":{\"staticBundling\":{}},\"rbg-Size\":\"config1\",\"resourceAllocation\":\"resourceAllocationType1\",\"tci-StatesToAddModList\":[{\"TCI-State\":{\"qcl-Type1\":{\"cell\":0,\"qcl-Type\":\"typeA\",\"referenceSignal\":{\"ssb\":1}},\"tci-StateId\":0}}]}}},\"pdsch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":2},\"nrofHARQ-ProcessesForPDSCH\":\"n16\"}},\"tag-Id\":0,\"uplinkConfig\":{\"firstActiveUplinkBWP-Id\":0,\"initialUplinkBWP\":{\"pucch-Config\":{\"setup\":{\"dl-DataToUL-ACK\":[{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4},{\"INTEGER\":4}],\"format2\":{\"setup\":{\"maxCodeRate\":\"zeroDot15\",\"simultaneousHARQ-ACK-CSI\":\"true\"}},\"pucch-PowerControl\":{\"deltaF-PUCCH-f0\":0,\"deltaF-PUCCH-f1\":0,\"deltaF-PUCCH-f2\":2,\"deltaF-PUCCH-f3\":0,\"p0-Set\":[{\"P0-PUCCH\":{\"p0-PUCCH-Id\":1,\"p0-PUCCH-Value\":0}}],\"pathlossReferenceRSs\":[{\"PUCCH-PathlossReferenceRS\":{\"pucch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"resourceSetToAddModList\":[{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":0,\"resourceList\":[{\"PUCCH-ResourceId\":1},{\"PUCCH-ResourceId\":2},{\"PUCCH-ResourceId\":3},{\"PUCCH-ResourceId\":4}]}},{\"PUCCH-ResourceSet\":{\"pucch-ResourceSetId\":1,\"resourceList\":[{\"PUCCH-ResourceId\":5}]}}],\"resourceToAddModList\":[{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":4,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":0,\"startingPRB\":4}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":1,\"startingPRB\":11}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":2,\"startingPRB\":12}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":3,\"startingPRB\":13}},{\"PUCCH-Resource\":{\"format\":{\"format0\":{\"initialCyclicShift\":0,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":4,\"startingPRB\":14}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":9,\"nrofSymbols\":1,\"startingSymbolIndex\":12}},\"pucch-ResourceId\":5,\"startingPRB\":4}},{\"PUCCH-Resource\":{\"format\":{\"format2\":{\"nrofPRBs\":5,\"nrofSymbols\":1,\"startingSymbolIndex\":13}},\"pucch-ResourceId\":6,\"startingPRB\":6}}],\"schedulingRequestResourceToAddModList\":[{\"SchedulingRequestResourceConfig\":{\"periodicityAndOffset\":{\"sl10\":0},\"resource\":0,\"schedulingRequestID\":0,\"schedulingRequestResourceId\":1}}]}},\"pusch-Config\":{\"setup\":{\"codebookSubset\":\"nonCoherent\",\"dataScramblingIdentityPUSCH\":708,\"dmrs-UplinkForPUSCH-MappingTypeA\":{\"setup\":{\"dmrs-AdditionalPosition\":\"pos1\",\"transformPrecodingDisabled\":{\"scramblingID0\":708}}},\"maxRank\":1,\"pusch-PowerControl\":{\"msg3-Alpha\":\"alpha1\",\"p0-AlphaSets\":[{\"P0-PUSCH-AlphaSet\":{\"alpha\":\"alpha1\",\"p0\":0,\"p0-PUSCH-AlphaSetId\":0}}],\"pathlossReferenceRSToAddModList\":[{\"PUSCH-PathlossReferenceRS\":{\"pusch-PathlossReferenceRS-Id\":0,\"referenceSignal\":{\"ssb-Index\":1}}}]},\"pusch-TimeDomainAllocationList\":{\"setup\":[{\"PUSCH-TimeDomainResourceAllocation\":{\"k2\":4,\"mappingType\":\"typeA\",\"startSymbolAndLength\":69}}]},\"resourceAllocation\":\"resourceAllocationType1\",\"transformPrecoder\":\"disabled\",\"txConfig\":\"codebook\"}},\"srs-Config\":{\"setup\":{\"srs-ResourceSetToAddModList\":[{\"SRS-ResourceSet\":{\"alpha\":\"alpha1\",\"p0\":-86,\"pathlossReferenceRS\":{\"ssb-Index\":1},\"resourceType\":{\"aperiodic\":{\"aperiodicSRS-ResourceTrigger\":1,\"csi-RS\":1,\"slotOffset\":1}},\"srs-PowerControlAdjustmentStates\":\"separateClosedLoop\",\"srs-ResourceIdList\":[{\"SRS-ResourceId\":0}],\"srs-ResourceSetId\":0,\"usage\":\"codebook\"}}],\"srs-ResourceToAddModList\":[{\"SRS-Resource\":{\"freqDomainPosition\":0,\"freqDomainShift\":0,\"freqHopping\":{\"b-SRS\":0,\"b-hop\":0,\"c-SRS\":0},\"groupOrSequenceHopping\":\"neither\",\"nrofSRS-Ports\":\"port1\",\"resourceMapping\":{\"nrofSymbols\":\"n1\",\"repetitionFactor\":\"n1\",\"startPosition\":0},\"resourceType\":{\"aperiodic\":{}},\"sequenceId\":0,\"spatialRelationInfo\":{\"referenceSignal\":{\"ssb-Index\":1},\"servingCellId\":0},\"srs-ResourceId\":0,\"transmissionComb\":{\"n4\":{\"combOffset-n4\":0,\"cyclicShift-n4\":0}}}}]}}},\"pusch-ServingCellConfig\":{\"setup\":{\"ext1\":{\"maxMIMO-Layers\":1}}}}}}}}},\"radioBearerConfig\":{\"drb-ToAddModList\":[{\"DRB-ToAddMod\":{\"cnAssociation\":{\"sdap-Config\":{\"defaultDRB\":true,\"mappedQoS-FlowsToAdd\":[{\"QFI\":1}],\"pdu-Session\":2,\"sdap-HeaderDL\":\"present\",\"sdap-HeaderUL\":\"present\"}},\"drb-Identity\":1,\"pdcp-Config\":{\"drb\":{\"discardTimer\":\"infinity\",\"headerCompression\":{\"notUsed\":null},\"pdcp-SN-SizeDL\":\"len18bits\",\"pdcp-SN-SizeUL\":\"len18bits\",\"statusReportRequired\":\"true\"},\"t-Reordering\":\"ms140\"},\"recoverPDCP\":\"true\"}}],\"srb-ToAddModList\":[{\"SRB-ToAddMod\":{\"srb-Identity\":1}},{\"SRB-ToAddMod\":{\"srb-Identity\":2}}]}}},\"rrc-TransactionIdentifier\":2}}}}}\n")
        out_MissingNBR_T = out_MissingNBR_T.withColumn("jsonDataNB", func.from_json(col("RRC_NR5G_message"), schemaNB))
        out_MissingNBR_T = out_MissingNBR_T.select('Event_Analysis_End_Time', 'Failed_Event_Number', 'Time',
                                                   'WorkOrderId',
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
        out_MissingNBR = out_MissingNBR.withColumn('NBR_Check', concat_ws("_", col('NBR_Check_0'), col('EventA3')))
        out_MissingNBR = out_MissingNBR.drop('NBR_Check_0', 'measId_MR', 'measId_RC', 'reportConfigId_m',
                                             'reportConfigId_evt', 'EventA3')
        out_MissingNBR = out_MissingNBR.sort(out_MissingNBR.Time.asc())
        out_MissingNBR = out_MissingNBR.filter(col('NBR_Check') != '')

        # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        # Code for \\\ SubWindow ///
        out_subWindow = out_MissingNBR.filter(out_MissingNBR.NBR_Check == 'Check1')
        subWindow1 = Window.partitionBy('Failed_Event_Number').orderBy('Time').rowsBetween(Window.unboundedPreceding,
                                                                                           Window.unboundedFollowing)
        out_subWindow = out_subWindow.withColumn('SWnd_StartTime', func.last('Time').over(subWindow1))
        out_subWindow = out_subWindow.select('Failed_Event_Number', 'SWnd_StartTime',
                                             'Event_Analysis_End_Time').distinct()

        # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\

        Condition_MEAS_NR2 = (df_MEAS_NR["Time"] >= out_subWindow["SWnd_StartTime"]) & (
                df_MEAS_NR["Time"] <= out_subWindow["Event_Analysis_End_Time"])
        out_MEAS_NR_sub = out_subWindow.join(df_MEAS_NR, Condition_MEAS_NR2, 'leftouter').sort(
            df_MEAS_NR.Time.asc())
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
        out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('SubWCheck', concat_ws('_', out_MEAS_NR_sub.Length_abc2,
                                                                            out_MEAS_NR_sub.RadioCondition))
        out_MEAS_NR_sub = out_MEAS_NR_sub.drop('Length_abc', 'Length_abc2', 'RadioCondition')
        out_MEAS_NR_sub = out_MEAS_NR_sub.filter(col('SubWCheck') != '')

        out_MissingNBR = out_MissingNBR.groupby('Failed_Event_Number').agg(
            collect_set(out_MissingNBR.NBR_Check).alias('NBR_Check'), )
        out_MissingNBR = out_MissingNBR.withColumn('NBR_Check', concat_ws('_', col('NBR_Check')))
        out_MissingNBR = out_MissingNBR.join(out_MEAS_NR_sub, ['Failed_Event_Number'], 'left')
        out_MissingNBR = out_MissingNBR.withColumn('NBR_CheckAll',
                                                   concat_ws('_', out_MissingNBR.NBR_Check, out_MissingNBR.SubWCheck))
        out_MissingNBR = out_MissingNBR.withColumn('N_Analysis',
                                                   when(out_MissingNBR.NBR_CheckAll == 'Check1_NCheck1_NCheck2',
                                                        'Handover failure').otherwise(None))
        out_MissingNBR = out_MissingNBR.drop('NBR_Check', 'SubWCheck', 'NBR_CheckAll')
        out_MissingNBR = out_MissingNBR.filter(col('N_Analysis').isNotNull())
        t_hof = t_hof.join(out_MissingNBR, ['Failed_Event_Number'], 'leftsemi')


        return out_MissingNBR, t_hof

    def f_rach_fail(self):
        """
        This function return these tags
        1- (when RACHT_NR5G_RACHReason == 0 then
        RACHA_NR5G_RACHResult == 0 then rach failure where only RACHReason = 0 'RACH failure')

        :param self.df_RACH_NR:
        :return out_RACH_NR:
        """
        df_RACH_NR = self.df_RACH_NR
        an_w = self.an_window()
        Condition_RACH_NR = (df_RACH_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_RACH_NR["Time"] <= an_w["Event_Analysis_End_Time"])
        out_RACH_NR = an_w.join(df_RACH_NR, Condition_RACH_NR)
        out_RACH_NR = out_RACH_NR.drop('F_Event_CallEvent', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time',
                                       'Failed_Event_Type', 'WorkOrderId', 'Device')
        out_RACH_NR = out_RACH_NR.sort(out_RACH_NR.Time.asc())
        out_RACH_NR = out_RACH_NR.withColumn('RaCheck1',
                                             when(out_RACH_NR.RACHT_NR5G_RACHReason == 0, 'RCheck1').otherwise(None)) \
            .withColumn('RaCheck2', when(out_RACH_NR.RACHA_NR5G_RACHResult == 0, 'RCheck2').otherwise(None))


        # tag time
        t_rf = out_RACH_NR.select('Failed_Event_Number', 'Time', 'RaCheck1')\
            .filter(col('RaCheck1') == 'RCheck1').sort('Time')
        t_rf = t_rf.sort('Time').dropDuplicates(['Failed_Event_Number'])
        t_rf = t_rf.select('Failed_Event_Number', 'Time', lit('RACH failure').alias('Tag_Name'))


        out_RACH_NR = out_RACH_NR.withColumn('RaCheck', concat_ws(', ', col('RaCheck1'), col('RaCheck2')))
        out_RACH_NR = out_RACH_NR.groupby('Failed_Event_Number').agg(
            collect_set(out_RACH_NR.RaCheck).alias('RaCheck'), )
        out_RACH_NR = out_RACH_NR.withColumn('RaCheck', concat_ws(', ', col('RaCheck')))
        out_RACH_NR = out_RACH_NR.filter(out_RACH_NR.RaCheck == 'RCheck1, ').withColumn('RaAnalysis',
                                                                                        lit('RACH failure'))
        out_RACH_NR = out_RACH_NR.drop('RaCheck')
        t_rf = t_rf.join(out_RACH_NR, ['Failed_Event_Number'], 'leftsemi')

        return out_RACH_NR, t_rf

    def f_pingpong_ho(self):
        """
        This function return these tags
        1- (when time between two Handovers is <= 5s then find three handovers having time <=5
        then tag as 'Ping_Pong Handover')

        :param out_Signalling:
        :return: out_Signalling
        """
        df_Signalling = self.df_Signalling
        an_w = self.an_window()
        Condition_png = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
        out_Signalling = an_w.join(df_Signalling, Condition_png)
        out_Signalling = out_Signalling.drop('Event_Analysis_Start_Time', 'Event_Analysis_End_Time',
                                             'Failed_Event_Type')
        out_Signalling = out_Signalling.sort(df_Signalling.Time.asc())
        cndfilter = ['5GNR rrcReconfiguration', '5GNR rrcReconfigurationComplete']
        out_Signalling = out_Signalling.filter((out_Signalling.RRC_NR5G_event.isin(cndfilter)) | (
            out_Signalling.RRC_NR5G_event.rlike('systemInformationBlockType1')))
        out_Signalling = out_Signalling.withColumn('NBR_Check_0',
                                                   when((out_Signalling.RRC_NR5G_event == '5GNR rrcReconfiguration') & (
                                                       out_Signalling.RRC_NR5G_message.contains(
                                                           'reconfigurationWithSync')),
                                                        'HO_Com_sent')
                                                   .when(
                                                       out_Signalling.RRC_NR5G_event == '5GNR rrcReconfigurationComplete',
                                                       'HO_Com_Rcv')
                                                   .when(out_Signalling.RRC_NR5G_event.rlike(
                                                       'systemInformationBlockType1'),
                                                       'HO_Com_cplt')
                                                   .otherwise(None))
        out_Signalling = out_Signalling.filter(out_Signalling.NBR_Check_0.isNotNull())


        # tag time
        t_pnp = out_Signalling.select('Failed_Event_Number', 'Time', 'NBR_Check_0')\
            .filter(col('NBR_Check_0') == 'HO_Com_sent').sort('Time')
        t_pnp = t_pnp.sort('Time').dropDuplicates(['Failed_Event_Number'])
        t_pnp = t_pnp.select('Failed_Event_Number', 'Time', lit('PingPong Handover').alias('Tag_Name')).sort('Time')



        out_Signalling = out_Signalling.drop('RRC_NR5G_event', 'RRC_NR5G_message')

        subWindow0 = Window.partitionBy('Device').orderBy('Time')
        out_Signalling = out_Signalling \
            .withColumn('Rcv', func.lead('NBR_Check_0', 1).over(subWindow0)) \
            .withColumn('cplt', func.lead('NBR_Check_0', 2).over(subWindow0)) \
            .withColumn('S_PCI', out_Signalling.RRC_NR5G_pci) \
            .withColumn('T_PCI', func.lead('RRC_NR5G_pci', 2).over(subWindow0))
        out_Signalling = out_Signalling \
            .withColumn('Complete_Cmd', concat_ws('|', col('NBR_Check_0'), col('Rcv'), col('cplt'))) \
            .withColumn('HO1_PCIs', concat_ws('|', col('S_PCI'), col('T_PCI')))
        out_Signalling = out_Signalling.drop('RRC_NR5G_pci', 'NBR_Check_0', 'Rcv', 'cplt', 'S_PCI', 'T_PCI')
        out_Signalling = out_Signalling.filter(out_Signalling.Complete_Cmd == 'HO_Com_sent|HO_Com_Rcv|HO_Com_cplt')
        out_Signalling = out_Signalling.drop('Complete_Cmd')
        subWindow1 = Window.partitionBy('Device').orderBy('Time')
        out_Signalling = out_Signalling \
            .withColumn('HO2_Time', func.lead('Time', 1).over(subWindow1)) \
            .withColumn('HO2_PCIs', func.lead('HO1_PCIs', 1).over(subWindow1))
        out_Signalling = out_Signalling.withColumn('Time_Delta_sec',
                                                   col('HO2_Time').cast('double') - col('Time').cast('double'))
        out_Signalling = out_Signalling.withColumn('cck',
                                                   when(out_Signalling.Time_Delta_sec <= 5, 'CC').otherwise(None))
        out_Signalling = out_Signalling.withColumn("next_cck",
                                                   func.lead("cck").over(Window.partitionBy('Device').orderBy("Time")))
        out_Signalling = out_Signalling.withColumn('PngPong_Analysis',
                                                   when(col('cck') == col('next_cck'), 'Ping_Pong Handover').otherwise(
                                                       None))
        out_Signalling = out_Signalling.filter(col('PngPong_Analysis') == 'PingPong Handover')
        out_Signalling = out_Signalling.drop('Time', 'WorkOrderId', 'Device', 'HO1_PCIs', 'HO2_Time', 'HO2_PCIs',
                                             'Time_Delta_sec', 'cck', 'next_cck')
        t_pnp = t_pnp.join(out_Signalling, ['Failed_Event_Number'], 'leftsemi')

        return out_Signalling, t_pnp

    def f_reestab_fail(self):
        """
        This function return this tag 'rrcReestablishment Failure'
        1- (when rrcReestablishmentRequest and (rrcSetup or rrcReestablishment)
        and (rrcReestablishmentComplete or rrcSetupComplete) are successful then
        these are rrcReestablishment successful otherwise fail and this function
        return only failures 'rrcReestablishment Failure')

        :param self.df_Signalling:
        :return out_reestab_fail:
        """
        df_Signalling = self.df_Signalling
        an_w = self.an_window()
        Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
        out_reestab_fail = an_w.join(df_Signalling, Condition_Signalling, 'left')
        out_reestab_fail = out_reestab_fail.drop('Failed_Event_Type', 'Event_Analysis_Start_Time',
                                                 'Event_Analysis_End_Time',
                                                 'WorkOrderId', 'Device', 'RRC_NR5G_pci', 'RRC_NR5G_message')
        out_reestab_fail = out_reestab_fail.withColumn('r_f_Check1', when(
            out_reestab_fail.RRC_NR5G_event == '5GNR rrcReestablishmentRequest', 'rfc_1').otherwise(None))



        # Tag Time
        ref_time = out_reestab_fail.select('Failed_Event_Number', 'Time', 'r_f_Check1')\
            .filter(col('r_f_Check1') == 'rfc_1').sort('Time')
        ref_time = ref_time.select('Failed_Event_Number', 'Time', lit('rrcReestablishment Failure').alias('Tag_Name'))\
            .sort('Time')
        ref_time = ref_time.sort('Time').dropDuplicates(['Failed_Event_Number'])


        out_reestab_fail = out_reestab_fail.withColumn('r_f_Check2', when(
            (out_reestab_fail.RRC_NR5G_event == '5GNR rrcSetup') |
            (out_reestab_fail.RRC_NR5G_event == '5GNR rrcReestablishment'), 'rfc_2').otherwise(None))
        out_reestab_fail = out_reestab_fail.withColumn('r_f_Check3', when(
            (out_reestab_fail.RRC_NR5G_event == '5GNR rrcReestablishmentComplete') |
            (out_reestab_fail.RRC_NR5G_event.rlike('rrcSetupComplete')), 'rfc_3').otherwise(None))
        out_reestab_fail = out_reestab_fail.withColumn('r_f_Check3_f',
                                                       concat_ws(',', col('r_f_Check1'), col('r_f_Check2'),
                                                                 col('r_f_Check3')))
        out_reestab_fail = out_reestab_fail.filter(col('r_f_Check3_f') != '')
        out_reestab_fail = out_reestab_fail.groupby('Failed_Event_Number').agg(
            collect_set(out_reestab_fail.r_f_Check3_f).alias('r_f_Check3_f'), )
        out_reestab_fail = out_reestab_fail.withColumn('r_f_Check3_f', concat_ws("|", col('r_f_Check3_f')))
        out_reestab_fail = out_reestab_fail.filter(out_reestab_fail.r_f_Check3_f.isin(['rfc_1', 'rfc_1|rfc_2'])) \
            .withColumn('ResFAnalysis', lit('rrcReestablishment Failure'))
        out_reestab_fail = out_reestab_fail.drop('r_f_Check3_f')
        ref_time = ref_time.join(out_reestab_fail, ['Failed_Event_Number'], 'leftsemi')

        return out_reestab_fail, ref_time

    def f_reestablishment(self):
        """
        This function return this tag 'rrcReestablishment'
        1- (when rrcReestablishmentRequest and (rrcSetup or rrcReestablishment)
        and (rrcReestablishmentComplete or rrcSetupComplete) are successful then
        these are rrcReestablishment successful otherwise fail and this function
        return only successful 'rrcReestablishment')

        :param self.df_Signalling:
        :return: out_reestablishment
        """
        df_Signalling = self.df_Signalling
        an_w = self.an_window()
        Condition_Signalling = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
        out_reestablishment = an_w.join(df_Signalling, Condition_Signalling, 'left')
        out_reestablishment = out_reestablishment.drop('Failed_Event_Type', 'Event_Analysis_Start_Time',
                                                       'Event_Analysis_End_Time',
                                                       'WorkOrderId', 'Device', 'RRC_NR5G_pci', 'RRC_NR5G_message')
        out_reestablishment = out_reestablishment.withColumn('r_f_Check1', when(
            out_reestablishment.RRC_NR5G_event == '5GNR rrcReestablishmentRequest', 'rfc_1').otherwise(None))


        # Tag Time
        ree_time = out_reestablishment.select('Failed_Event_Number', 'Time', 'r_f_Check1')\
            .filter(col('r_f_Check1') == 'rfc_1').sort('Time')
        ree_time = ree_time.select('Failed_Event_Number', 'Time', lit('rrcReestablishment').alias('Tag_Name'))\
            .sort('Time')
        ree_time = ree_time.sort('Time').dropDuplicates(['Failed_Event_Number'])



        out_reestablishment = out_reestablishment.withColumn('r_f_Check2', when(
            (out_reestablishment.RRC_NR5G_event == '5GNR rrcSetup') |
            (out_reestablishment.RRC_NR5G_event == '5GNR rrcReestablishment'), 'rfc_2').otherwise(None))
        out_reestablishment = out_reestablishment.withColumn('r_f_Check3', when(
            (out_reestablishment.RRC_NR5G_event == '5GNR rrcReestablishmentComplete') |
            (out_reestablishment.RRC_NR5G_event.rlike('rrcSetupComplete')), 'rfc_3').otherwise(None))
        out_reestablishment = out_reestablishment.withColumn('r_f_Check3_f',
                                                             concat_ws('|', col('r_f_Check1'), col('r_f_Check2'),
                                                                       col('r_f_Check3')))
        out_reestablishment = out_reestablishment.filter(col('r_f_Check3_f') != '')
        out_reestablishment = out_reestablishment.groupby('Failed_Event_Number').agg(
            collect_set(out_reestablishment.r_f_Check3_f).alias('r_f_Check3_f'))
        out_reestablishment = out_reestablishment.withColumn('r_f_Check3_f', concat_ws("|", col('r_f_Check3_f')))
        out_reestablishment = out_reestablishment.filter(out_reestablishment.r_f_Check3_f == 'rfc_1|rfc_2|rfc_3') \
            .withColumn('ResmntAnalysis', lit('rrcReestablishment'))
        out_reestablishment = out_reestablishment.drop('r_f_Check3_f')
        ree_time = ree_time.join(out_reestablishment, ['Failed_Event_Number'], 'leftsemi')

        return out_reestablishment, ree_time

    def f_missing_neighbor_relation(self):
        """
        This function return this tag 'Missing neighbor relation '

        :param df_Signalling:
        :return: out_reestablishment
        """
        df_Signalling = self.df_Signalling
        df_MEAS_NR = self.df_MEAS_NR
        an_w = self.an_window()
        Condition_Signalling2 = (df_Signalling["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_Signalling["Time"] <= an_w["Event_Analysis_End_Time"])
        out_MissingNBR = an_w.join(df_Signalling, Condition_Signalling2, 'left')
        out_MissingNBR = out_MissingNBR.drop('F_Event_CallEvent', 'Event_Analysis_Start_Time', 'Failed_Event_Type')
        out_MissingNBR = out_MissingNBR.sort(out_MissingNBR.Time.asc())
        out_MissingNBR = out_MissingNBR.filter((out_MissingNBR.RRC_NR5G_event == '5GNR measurementReport') |
                                               (out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfiguration'))
        out_MissingNBR = out_MissingNBR.withColumn('NBR_Check_0',
                                                   when((out_MissingNBR.RRC_NR5G_event == '5GNR measurementReport')
                                                        & (out_MissingNBR.RRC_NR5G_message.contains(
                                                       'measResultNeighCells')), 'Check1')
                                                   .when((out_MissingNBR.RRC_NR5G_event == '5GNR rrcReconfiguration')
                                                         & (out_MissingNBR.RRC_NR5G_message.contains(
                                                       'reconfigurationWithSync')), 'Check2')
                                                   .otherwise(None))



        # tag time
        t_mnr = out_MissingNBR.select('Failed_Event_Number', 'Time', 'NBR_Check_0')\
            .filter(col('NBR_Check_0') == 'Check1').sort('Time')
        t_mnr = t_mnr.sort('Time').dropDuplicates(['Failed_Event_Number'])
        t_mnr = t_mnr.select('Failed_Event_Number', 'Time', lit('Missing neighbor relation').alias('Tag_Name')).sort('Time')





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

        out_MissingNBR = out_MissingNBR.withColumn('NBR_Check', concat_ws("_", col('NBR_Check_0'), col('EventA3')))
        out_MissingNBR = out_MissingNBR.drop('NBR_Check_0', 'measId_MR', 'measId_RC', 'reportConfigId_m',
                                             'reportConfigId_evt', 'EventA3')
        out_MissingNBR = out_MissingNBR.sort(out_MissingNBR.Time.asc())

        out_MissingNBR = out_MissingNBR.filter(col('NBR_Check') != '')


        # /\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\
        # Code for \\\ SubWindow ///
        out_subWindow = out_MissingNBR.filter(out_MissingNBR.NBR_Check == 'Check1')
        subWindow1 = Window.partitionBy('Failed_Event_Number').orderBy('Time').rowsBetween(Window.unboundedPreceding,
                                                                                           Window.unboundedFollowing)
        out_subWindow = out_subWindow.withColumn('SWnd_StartTime', func.last('Time').over(subWindow1))
        out_subWindow = out_subWindow.select('Failed_Event_Number', 'SWnd_StartTime',
                                             'Event_Analysis_End_Time').distinct()

        Condition_MEAS_NR2 = (df_MEAS_NR["Time"] >= out_subWindow["SWnd_StartTime"]) & (
                df_MEAS_NR["Time"] <= out_subWindow["Event_Analysis_End_Time"])
        out_MEAS_NR_sub = out_subWindow.join(df_MEAS_NR, Condition_MEAS_NR2, 'leftouter').sort(
            df_MEAS_NR.Time.asc())
        out_MEAS_NR_sub = out_MEAS_NR_sub.drop('Failed_Event_Type')
        out_MEAS_NR_sub = out_MEAS_NR_sub.groupby('Failed_Event_Number').agg(
            func.expr('collect_set(MEAS_NR5G_PCC_PCI)').alias('NR5G_PCC_PCI_Last'),
            func.expr('avg(MEAS_NR5G_PCC_RSRP_AVG)').alias('NR5G_PCC_RSRP_Last'),
            func.expr('avg(MEAS_NR5G_PCC_SNR_AVG)').alias('NR5G_PCC_SINR_Last'), ) \
            .sort(out_MEAS_NR_sub.Failed_Event_Number.asc())
        out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('NR5G_PCC_PCI_Last', concat_ws(", ", col('NR5G_PCC_PCI_Last')))
        out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('Length_abc', length(out_MEAS_NR_sub.NR5G_PCC_PCI_Last))
        out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('Length_abc2', when(out_MEAS_NR_sub.Length_abc < 5, 'NCheck1')
                                                     .otherwise(None))
        out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('RadioCondition',
                                                     when(out_MEAS_NR_sub.NR5G_PCC_RSRP_Last < -110, 'NCheck2')
                                                     .when(out_MEAS_NR_sub.NR5G_PCC_SINR_Last < 0, 'NCheck2')
                                                     .otherwise(None))
        out_MEAS_NR_sub = out_MEAS_NR_sub.withColumn('SubWCheck', concat_ws('_', out_MEAS_NR_sub.Length_abc2,
                                                                            out_MEAS_NR_sub.RadioCondition))
        out_MEAS_NR_sub = out_MEAS_NR_sub.drop('Length_abc', 'Length_abc2', 'RadioCondition')
        out_MEAS_NR_sub = out_MEAS_NR_sub.filter(col('SubWCheck') != '')
        out_MissingNBR = out_MissingNBR.groupby('Failed_Event_Number').agg(
            collect_set(out_MissingNBR.NBR_Check).alias('NBR_Check'), )
        out_MissingNBR = out_MissingNBR.withColumn('NBR_Check', concat_ws('_', col('NBR_Check')))

        out_MissingNBR = out_MissingNBR.join(out_MEAS_NR_sub, ['Failed_Event_Number'], 'left')
        out_MissingNBR = out_MissingNBR.withColumn('NBR_CheckAll',
                                                   concat_ws('_', out_MissingNBR.NBR_Check, out_MissingNBR.SubWCheck))
        out_MissingNBR = out_MissingNBR.withColumn('MNR_Analysis',
                                                   when(out_MissingNBR.NBR_CheckAll == 'Check1_NCheck1_NCheck2',
                                                        'Missing neighbor relation')
                                                   .otherwise(None))
        out_MissingNBR = out_MissingNBR.drop('NBR_Check', 'SubWCheck', 'NBR_CheckAll')
        out_MissingNBR = out_MissingNBR.filter(col('MNR_Analysis').isNotNull())
        out_MissingNBR = out_MissingNBR.drop('NR5G_PCC_PCI_Last', 'NR5G_PCC_RSRP_Last', 'NR5G_PCC_SINR_Last')
        t_mnr = t_mnr.join(out_MissingNBR, ['Failed_Event_Number'], 'leftsemi')

        return out_MissingNBR, t_mnr

    def f_highbler(self):
        """
        This function return these tags
        1- (Calculate average of BLER on complete windowTag High BLER if average >10)

        :param :
        :return: out_Signalling_NR
        """
        df_PHYDL_NR = self.df_PHYDL_NR
        an_w = self.an_window()
        Condition_hbler = (df_PHYDL_NR["Time"] >= an_w["Event_Analysis_Start_Time"]) & (
                df_PHYDL_NR["Time"] <= an_w["Event_Analysis_End_Time"])
        out_PHYDL_NR = an_w.join(df_PHYDL_NR, Condition_hbler)
        out_PHYDL_NR = out_PHYDL_NR.select('Failed_Event_Number', 'Time', 'PHYDL_NR5G_PCC_bler')
        out_PHYDL_NR = out_PHYDL_NR.filter(col('PHYDL_NR5G_PCC_bler').isNotNull())


        # Tag time
        t_hb = out_PHYDL_NR.filter(col('PHYDL_NR5G_PCC_bler') > 10).sort('Time')
        t_hb = t_hb.sort('Time')
        t_hb = t_hb.dropDuplicates(['Failed_Event_Number'])
        t_hb = t_hb.sort('Time')
        t_hb = t_hb.select('Failed_Event_Number', 'Time', lit('High Physical DL BLER').alias('Tag_Name')).sort('Time')



        out_PHYDL_NR = out_PHYDL_NR.groupby('Failed_Event_Number').agg(
            func.expr('round(avg(PHYDL_NR5G_PCC_bler), 2)').alias('PHYDL_NR5G_PCC_BLER_Avg'))
        out_PHYDL_NR = out_PHYDL_NR.filter(col('PHYDL_NR5G_PCC_BLER_Avg') > 10)
        out_PHYDL_NR = out_PHYDL_NR.withColumn('HB_Analysis', lit('High Physical DL BLER'))
        t_hb = t_hb.join(out_PHYDL_NR, ['Failed_Event_Number'], 'leftsemi')

        return out_PHYDL_NR, t_hb

    def f_ref_cols(self):
        """
        This function return only reference columns from event_NR table

        :param self.df_rc:
        :return out_self.df_rc:
        """
        df_rc = self.df_rc
        an_w = self.an_window()
        out_df_rc = df_rc.join(an_w, (df_rc["Time"] == an_w["Event_Analysis_End_Time"])) \
            .sort(df_rc.Time.asc())
        out_df_rc = out_df_rc.drop('Failed_Event_Type', 'Event_Analysis_Start_Time', 'Event_Analysis_End_Time')
        return out_df_rc

    def f_final_df(self):
        """
        This function join all the functions output and combine for
        final analysis column.

        :return out_FinalDF:
        """
        an_w = self.an_window()
        ref_cols = self.f_ref_cols()

        df1, t1 = self.f_radio_cnd()
        df2, t2 = self.f_lost_ue()
        df3, t3 = self.f_msing_sig()
        df4, t4 = self.f_raoming()
        df5, t5 = self.f_ho_failure()
        df6, t6 = self.f_rach_fail()
        df7, t7 = self.f_pingpong_ho()
        df8, t8 = self.f_reestab_fail()
        df9, t9 = self.f_reestablishment()
        df10, t10 = self.f_missing_neighbor_relation()
        df11, t11 = self.f_highbler()

        out_FinalDF = df1\
            .join(df2, ['Failed_Event_Number'], 'full') \
            .join(df3, ['Failed_Event_Number'], 'full') \
            .join(df4, ['Failed_Event_Number'], 'full') \
            .join(df5, ['Failed_Event_Number'], 'full') \
            .join(df6, ['Failed_Event_Number'], 'full') \
            .join(df7, ['Failed_Event_Number'], 'full') \
            .join(df8, ['Failed_Event_Number'], 'full') \
            .join(df9, ['Failed_Event_Number'], 'full') \
            .join(df10, ['Failed_Event_Number'], 'full') \
            .join(df11, ['Failed_Event_Number'], 'full')

        out_FinalDF = out_FinalDF.drop('radio_Analysis',
                                       'lost_Analysis',
                                       'MsgSig_Analysis',
                                       'romg_Analysis',
                                       'N_Analysis',
                                       'RaAnalysis',
                                       'PngPong_Analysis',
                                       'ResFAnalysis',
                                       'ResmntAnalysis',
                                       'MNR_Analysis',
                                       'HB_Analysis'
                                       )


        df1.unpersist()
        df2.unpersist()
        df3.unpersist()
        df4.unpersist()
        df5.unpersist()
        df6.unpersist()
        df7.unpersist()
        df8.unpersist()
        df9.unpersist()
        df10.unpersist()
        df11.unpersist()

        ref_df = an_w.join(ref_cols, ['Failed_Event_Number'], 'full')
        an_w.unpersist()
        ref_cols.unpersist()
        out_FinalDF = ref_df.join(out_FinalDF, ['Failed_Event_Number'], 'full')
        ref_df.unpersist()

        tag_time = t1.union(t2).union(t3).union(t4).union(t5).union(t6).union(t7)\
            .union(t8).union(t9).union(t10).union(t11).distinct().sort('Time')
        t1.unpersist()
        t2.unpersist()
        t3.unpersist()
        t4.unpersist()
        t5.unpersist()
        t6.unpersist()
        t7.unpersist()
        t8.unpersist()
        t9.unpersist()
        t10.unpersist()
        t11.unpersist()

        wn1 = Window.partitionBy('Failed_Event_Number').orderBy('Time')
        tag_time = tag_time.withColumn('new_tag', func.collect_list('Tag_Name').over(wn1))
        tag_time = tag_time.select('Failed_Event_Number', 'new_tag').sort(col('Time').desc())
        tag_time = tag_time.dropDuplicates(['Failed_Event_Number'])
        tag_time = tag_time.withColumn('Analysis', concat_ws(', ', 'new_tag'))
        tag_time = tag_time.select('Failed_Event_Number', 'Analysis')

        out_FinalDF = out_FinalDF.join(tag_time, ['Failed_Event_Number'], 'left')
        out_FinalDF = out_FinalDF.fillna('Unidentified', ['Analysis'])

        return out_FinalDF


df_device3 = DataFailure('IPERF3_DL').f_final_df()
d1 = df_device3.toPandas()
df_device3.unpersist()


# o_final_df = df_device3.unionAll(df_device4)
# df_device3.unpersist()
# df_device4.unpersist()
# o_final_df = o_final_df.select('Failed_Event_Number', 'Failed_Event_Type', 'WorkOrderId', 'Device', 'Analysis')
# o_final_df.show()
# o_final_df.write.mode('overwrite').options(header='True').csv('/home/spark-master/Public/Raheel/final/fail')

df_device4 = DataFailure('IPERF3_UL').f_final_df()
d2 = df_device4.toPandas()
df_device4.unpersist()
pdFrame = pd.concat([d1, d2])

# pdFrame = o_final_df.toPandas()
pdFrame["Time"] = pdFrame["Time"].dt.strftime("%Y-%m-%d %X.%f")
pdFrame["Event_Analysis_Start_Time"] = pdFrame["Event_Analysis_Start_Time"].dt.strftime("%Y-%m-%d %X.%f")
pdFrame["Event_Analysis_End_Time"] = pdFrame["Event_Analysis_End_Time"].dt.strftime("%Y-%m-%d %X.%f")
pdFrame = pdFrame.replace({np.nan: None})
com.output(pdFrame, "py_Data_FailureAnalysis", cols)
print("\n--- Query process time is %s minutes ---\n" % ((time.time() - start_time)/60))





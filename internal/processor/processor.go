package processor

import (
	"crypto/sha256"
	"encoding/hex"
	"time"

	"github.com/mata-elang-stable/event-stream-aggr/internal/logger"
	"github.com/mata-elang-stable/event-stream-aggr/internal/pb"
	"github.com/mata-elang-stable/event-stream-aggr/internal/types"
)

var log = logger.GetLogger()

// parserDate converts unix timestamp to human-readable date in yy/MM/dd-HH:mm:ss.SSSSSS format
func parseUnixMicroTimestampToString(t int64) string {
	return time.UnixMicro(t).UTC().Format("2006-01-02T15:04:05.999Z")
}

// GetRawDataFromMetrics converts the protobuf message to the internal data structure.
func GetRawDataFromMetrics(data *pb.SensorEvent, metric *pb.Metric) *types.SnortAlert {
	if data == nil || metric == nil {
		return nil
	}

	priorityStr := ParsePriority(data.SnortPriority)
	sentAtStr := parseUnixMicroTimestampToString(data.EventSentAt)
	readAtStr := parseUnixMicroTimestampToString(data.EventReadAt)
	receivedAtStr := parseUnixMicroTimestampToString(data.EventReceivedAt)

	return &types.SnortAlert{
		Metadata: types.Metadata{
			SensorID:      data.SensorId,
			SensorVersion: data.SensorVersion,
			HashSHA256:    data.EventHashSha256,
			SentAt:        sentAtStr,
			ReadAt:        readAtStr,
			ReceivedAt:    receivedAtStr,
		},
		Action:         data.SnortAction,
		Base64Data:     metric.SnortBase64Data,
		Classification: data.SnortClassification,
		ClientBytes:    metric.SnortClientBytes,
		ClientPkts:     metric.SnortClientPkts,
		Direction:      data.SnortDirection,
		DstAddr:        metric.SnortDstAddress,
		DstAp:          metric.SnortDstAp,
		DstPort:        metric.SnortDstPort,
		EthDst:         metric.SnortEthDst,
		EthLen:         metric.SnortEthLen,
		EthSrc:         metric.SnortEthSrc,
		EthType:        metric.SnortEthType,
		FlowStartTime:  metric.SnortFlowstartTime,
		GeneveVNI:      metric.SnortGeneveVni,
		GID:            data.SnortRuleGid,
		ICMPCode:       metric.SnortIcmpCode,
		ICMPID:         metric.SnortIcmpId,
		ICMPSeq:        metric.SnortIcmpSeq,
		ICMPType:       metric.SnortIcmpType,
		Interface:      data.SnortInterface,
		IPID:           metric.SnortIpId,
		IPLen:          metric.SnortIpLength,
		MPLS:           metric.SnortMpls,
		Message:        data.SnortMessage,
		PktGen:         metric.SnortPktGen,
		PktLen:         metric.SnortPktLength,
		PktNum:         metric.SnortPktNumber,
		Priority:       data.SnortPriority,
		PriorityStr:    priorityStr,
		Protocol:       data.SnortProtocol,
		Revision:       data.SnortRuleRev,
		RuleID:         data.SnortRule,
		Seconds:        data.SnortSeconds,
		ServerBytes:    metric.SnortServerBytes,
		ServerPkts:     metric.SnortServerPkts,
		Service:        data.SnortService,
		SGT:            metric.SnortSgt,
		SID:            data.SnortRuleSid,
		SrcAddr:        metric.SnortSrcAddress,
		SrcAp:          metric.SnortSrcAp,
		SrcPort:        metric.SnortSrcPort,
		Target:         metric.SnortTarget,
		TCPAck:         metric.SnortTcpAck,
		TCPFlags:       metric.SnortTcpFlags,
		TCPLen:         metric.SnortTcpLen,
		TCPSeq:         metric.SnortTcpSeq,
		TCPWin:         metric.SnortTcpWin,
		TOS:            data.SnortTypeOfService,
		TTL:            metric.SnortTimeToLive,
		UDPLen:         metric.SnortUdpLength,
		VLAN:           metric.SnortVlan,
	}
}

func GetHashKeyData(data string) string {
	return generateHashSHA256(data)
}

func generateHashSHA256(data string) string {
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:])
}

func roundTime(t int64, roundSeconds int64) int64 {
	return (t / roundSeconds) * roundSeconds
}

func ParsePriority(priority int64) string {
	switch priority {
	case 1:
		return "High"
	case 2:
		return "Medium"
	case 3:
		return "Low"
	default:
		return "Informational"
	}
}

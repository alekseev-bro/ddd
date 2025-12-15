package qos

type Ordering uint

const (
	Ordered Ordering = iota
	Unordered
)

type Delivery uint

const (
	AtLeastOnce Delivery = iota
	AtMostOnce
)

type QoS struct {
	Ordering Ordering
	Delivery Delivery
}

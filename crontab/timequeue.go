package crontab

import (
	"container/heap"
	"sync"
	"time"

	"gopkg.in/mgo.v2/bson"
)

type GetIder interface {
	GetId() bson.ObjectId
}

type TimeQueue struct {
	pq      PriorityQueue
	itemMap map[bson.ObjectId]*Item
	s       sync.Mutex
}

func NewTimeQueue() *TimeQueue {
	return &TimeQueue{
		itemMap: make(map[bson.ObjectId]*Item),
	}
}

func (c *TimeQueue) Update(obj GetIder, when time.Time) bool {
	c.s.Lock()
	defer c.s.Unlock()

	return c.update(obj, when)
}

func (c *TimeQueue) update(obj GetIder, when time.Time) bool {
	item := c.itemMap[obj.GetId()]
	if item == nil {
		return false
	}
	item.time = when
	item.value = obj
	heap.Fix(&c.pq, item.index)
	return true
}

func (c *TimeQueue) Upsert(obj GetIder, when time.Time) (exists bool) {
	c.s.Lock()
	defer c.s.Unlock()
	if !c.update(obj, when) {
		c.push(obj, when)
		return false
	}
	return true
}

func (c *TimeQueue) Push(obj GetIder, when time.Time) {
	c.s.Lock()
	defer c.s.Unlock()
	c.push(obj, when)
}

func (c *TimeQueue) push(obj GetIder, when time.Time) {
	item := &Item{
		value: obj,
		time:  when,
		index: len(c.pq),
	}
	c.itemMap[obj.GetId()] = item
	heap.Push(&c.pq, item)
	heap.Fix(&c.pq, item.index)
}

func (c *TimeQueue) Pop(when time.Time) (GetIder, time.Duration) {
	c.s.Lock()
	defer c.s.Unlock()

	if len(c.pq) == 0 {
		return nil, 0
	}

	pop := heap.Pop(&c.pq)
	item := pop.(*Item)
	duration := when.Sub(item.time)
	if duration <= 0 {
		return nil, duration
	}
	delete(c.itemMap, item.value.GetId())
	return item.value, duration
}

type PriorityQueue []*Item

func (p PriorityQueue) Len() int {
	return len(p)
}
func (p PriorityQueue) Less(i, j int) bool {
	return p[i].time.Before(p[j].time)
}
func (p PriorityQueue) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
	p[i].index = i
	p[j].index = j
}
func (p *PriorityQueue) Push(x interface{}) {
	n := len(*p)
	item := x.(*Item)
	item.index = n
	*p = append(*p, item)
}
func (p *PriorityQueue) Pop() interface{} {
	old := *p
	n := len(old)
	item := old[n-1]
	item.index = -1
	*p = old[0 : n-1]
	return item
}

type Item struct {
	value GetIder
	time  time.Time
	index int
}

package connection

type AOFConnection struct {
	dbIndex int
}

func NewAOFConnection(db int) *AOFConnection {
	return &AOFConnection{dbIndex: db}
}

func (c *AOFConnection) GetDBIndex() int {
	return c.dbIndex
}

func (c *AOFConnection) SelectDB(i int) {
	c.dbIndex = i
}

func (c *AOFConnection) Write(b []byte) (int, error) {
	return len(b), nil
}

func (c *AOFConnection) IsFake() bool {
	return true
}

func (c *AOFConnection) Close() error {
	return nil
}

func (c *AOFConnection) IsClosed() bool {
	return false
}

func (c *AOFConnection) RemoteAddr() string {
	return "local:aof"
}

func (c *AOFConnection) SetSlave() {}

func (c *AOFConnection) IsSlave() bool {
	return false
}

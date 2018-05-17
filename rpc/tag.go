package rpc

const ntag = 255

func (c *Client) aqcuireTag() uint8 {
	if c.tags == nil {
		c.tags = make(chan uint8, ntag)
		for i := uint8(0); i < ntag; i++ {
			c.tags <- i
		}
	}

	return <-c.tags
}

func (c *Client) releaseTag(tag uint8) {
	c.tags <- tag
}

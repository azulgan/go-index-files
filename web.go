package main

import (
	"github.com/gin-gonic/gin"
	"github.com/olivere/elastic/v7"
)

func web(client *elastic.Client, config *Config) {
	router := gin.Default()
	router.LoadHTMLGlob("templates/*")

	// Query string parameters are parsed using the existing underlying request object.
	// The request responds to a url matching:  /welcome?firstname=Jane&lastname=Doe
	//router.GET("/page", func(c *gin.Context) {
	//	name := c.DefaultQuery("name", "")
	//	page := load(client, config.Es.Index, name)
	//	if page != nil {
	//		reader := strings.NewReader(page.Message)
	//		contentLength := int64(len(page.Message))
	//		contentType := "text/html"
	//		c.DataFromReader(http.StatusOK, contentLength, contentType, reader, nil)
	//	} else {
	//		c.String(http.StatusNotFound, "No page %s found", name)
	//	}
	//})
	//router.GET("/author", func(c *gin.Context) {
	//	name := c.DefaultQuery("name", "")
	//	pages := pageByAuthor(client, config.Es.Index, name)
	//	if pages != nil {
	//		c.HTML(http.StatusOK, "index.tmpl", gin.H{
	//			"list": pages,
	//		})
	//	} else {
	//		c.String(http.StatusNotFound, "No page %s found", name)
	//	}
	//})
	//router.GET("/keyword", func(c *gin.Context) {
	//	name := c.DefaultQuery("name", "")
	//	pages := pageByKeyword(client, config.Es.Index, name)
	//	if pages != nil {
	//		c.HTML(http.StatusOK, "index.tmpl", gin.H{
	//			"list": pages,
	//		})
	//	} else {
	//		c.String(http.StatusNotFound, "No page %s found", name)
	//	}
	//})
	router.Run(":8080")
}


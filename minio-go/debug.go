package minio

import (
	"fmt"
	"io/ioutil"
	"log"

	"os"

	"strconv"
	"strings"

	"net/http"

	"github.com/minio/cli"
	"github.com/minio/mc/pkg/httptracer"
)

var debugClient *Client

// Debug - entry.
func Debug() {
	app := cli.NewApp()
	app.Usage = "Minio debugger"
	app.Author = "Minio.io"
	app.Flags = []cli.Flag{
		cli.StringFlag{
			Name: "endpoint",
		},
		cli.StringFlag{
			Name: "accesskey",
		},
		cli.StringFlag{
			Name: "secretkey",
		},
		cli.BoolFlag{
			Name: "secure",
		},
		cli.BoolFlag{
			Name: "trace",
		},
	}
	app.Before = debugMain
	app.Commands = []cli.Command{
		{
			Name:  "multipart",
			Usage: "Multipart related operations",
			Action: func(ctx *cli.Context) {
				fmt.Println("here")
				cli.ShowCommandHelp(ctx, "")
			},
			Subcommands: []cli.Command{
				{
					Name:   "new",
					Usage:  "New multipart upload",
					Action: debugNewMultipart,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "bucket",
							Usage: "Bucket name",
						},
						cli.StringFlag{
							Name:  "object",
							Usage: "Object name",
						},
					},
				},
				{
					Name:   "upload",
					Usage:  "Upload part",
					Action: debugUploadPart,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name:  "bucket",
							Usage: "Bucket name",
						},
						cli.StringFlag{
							Name:  "object",
							Usage: "Object name",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
						cli.IntFlag{
							Name: "number",
						},
						cli.StringFlag{
							Name: "file",
						},
					},
				},
				{
					Name:   "complete",
					Usage:  "Complete multipart",
					Action: debugCompleteMultipart,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "object",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
					},
				},
				{
					Name:   "listuploads",
					Usage:  "List incomplete uploads",
					Action: debugListMultipartUploads,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "prefix",
						},
						cli.StringFlag{
							Name: "keymarker",
						},
						cli.StringFlag{
							Name: "uploadidmarker",
						},
						cli.BoolFlag{
							Name: "delimiter",
						},
						cli.IntFlag{
							Name: "maxuploads",
						},
					},
				},
				{
					Name:   "listparts",
					Usage:  "List parts",
					Action: debugListUploadParts,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "object",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
						cli.IntFlag{
							Name: "partmarker",
						},
						cli.IntFlag{
							Name: "maxparts",
						},
					},
				},
				{
					Name:   "abort",
					Usage:  "Abort multipart upload",
					Action: debugAbortMultipartUpload,
					Flags: []cli.Flag{
						cli.StringFlag{
							Name: "bucket",
						},
						cli.StringFlag{
							Name: "object",
						},
						cli.StringFlag{
							Name: "uploadid",
						},
					},
				},
			},
		},
	}
	app.RunAndExitOnError()
}

func debugMain(ctx *cli.Context) error {
	endpoint := ctx.GlobalString("endpoint")
	secure := ctx.GlobalBool("secure")
	accessKey := ctx.GlobalString("accesskey")
	secretKey := ctx.GlobalString("secretkey")
	trace := ctx.GlobalBool("trace")
	transport := http.DefaultTransport
	if trace {
		transport = httptracer.GetNewTraceTransport(newTraceV4(), http.DefaultTransport)
	}
	var err error
	debugClient, err = New(endpoint, accessKey, secretKey, secure)
	if err != nil {
		fmt.Println(err)
		cli.ShowCommandHelp(ctx, "")
		return err
	}
	debugClient.SetCustomTransport(transport)
	return nil
}

func debugNewMultipart(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	result, err := debugClient.initiateMultipartUpload(bucketName, objectName, "")
	if err != nil {
		log.Fatal(err)
		cli.ShowCommandHelp(ctx, "")
	}
	fmt.Println(result.UploadID)
}

func debugUploadPart(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	partNum := ctx.Int("number")
	filePath := ctx.String("file")
	f, err := os.Open(filePath)
	if err != nil {
		log.Fatal(err)
	}
	md5sum, sha256sum, partSize, err := debugClient.hashCopy(ioutil.Discard, f)
	if err != nil {
		log.Fatal(err)
	}
	_, err = f.Seek(0, 1)
	if err != nil {
		log.Fatal(err)
	}
	part, err := debugClient.uploadPart(bucketName, objectName, uploadID, f, partNum, md5sum, sha256sum, partSize)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(part)
}

func debugCompleteMultipart(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	parts := ctx.Args()
	var completeUpload completeMultipartUpload
	for _, part := range parts {
		split := strings.Split(part, ".")
		partNum, err := strconv.Atoi(split[0])
		if err != nil {
			log.Fatal(err)
		}
		md5sum := split[1]
		completeUpload.Parts = append(completeUpload.Parts, completePart{
			PartNumber: partNum,
			ETag:       md5sum,
		})
	}
	result, err := debugClient.completeMultipartUpload(bucketName, objectName, uploadID, completeUpload)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(result)
}

func debugListMultipartUploads(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	prefix := ctx.String("prefix")
	keyMarker := ctx.String("keymarker")
	uploadIDMarker := ctx.String("uploadidmarker")
	delimiter := ""
	maxuploads := ctx.Int("maxuploads")
	if ctx.Bool("delimiter") {
		delimiter = "/"
	}
	result, err := debugClient.listMultipartUploadsQuery(bucketName, keyMarker, uploadIDMarker, prefix, delimiter, maxuploads)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("bucket : ", result.Bucket)
	fmt.Println("keymarker : ", result.KeyMarker)
	fmt.Println("uploadidmarker : ", result.UploadIDMarker)
	fmt.Println("nextkeymarker : ", result.NextKeyMarker)
	fmt.Println("nextuploadidmarker : ", result.NextUploadIDMarker)
	fmt.Println("encodingtype : ", result.EncodingType)
	fmt.Println("maxuploads : ", result.MaxUploads)
	fmt.Println("istruncated : ", result.IsTruncated)
	fmt.Println("prefix : ", result.Prefix)
	fmt.Println("delimiter : ", result.Delimiter)
	fmt.Println("uploads :")
	for i, upload := range result.Uploads {
		fmt.Printf("  %d : %s, %s\n", i+1, upload.Key, upload.UploadID)
	}
	fmt.Println("commonprefixes : ")
	for i, p := range result.CommonPrefixes {
		fmt.Printf("  %d : %s\n", i+1, p)
	}
}

func debugListUploadParts(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	partmarker := ctx.Int("partmarker")
	maxParts := ctx.Int("maxparts")
	result, err := debugClient.listObjectPartsQuery(bucketName, objectName, uploadID, partmarker, maxParts)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("bucket : ", result.Bucket)
	fmt.Println("object : ", result.Key)
	fmt.Println("uploadid : ", result.UploadID)
	fmt.Println("nextpartmarker : ", result.NextPartNumberMarker)
	fmt.Println("istruncated : ", result.IsTruncated)
	fmt.Println("parts :")
	for i, part := range result.ObjectParts {
		fmt.Printf("  %d : %d %s %d\n", i+1, part.PartNumber, part.ETag, part.Size)
	}
}

func debugAbortMultipartUpload(ctx *cli.Context) {
	bucketName := ctx.String("bucket")
	objectName := ctx.String("object")
	uploadID := ctx.String("uploadid")
	err := debugClient.abortMultipartUpload(bucketName, objectName, uploadID)
	if err != nil {
		log.Fatal(err)
	}
}

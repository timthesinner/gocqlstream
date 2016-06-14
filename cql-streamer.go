//Package gocqlstream by TimTheSinner
package gocqlstream

/**
 * Copyright (c) 2016 TimTheSinner All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"bufio"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"math"
	"mime"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"
)

const minBlockSize = 4096 // we settle for 4KB
var _range = regexp.MustCompile(`bytes=(\d+)-(\d*)`)

type StreamDAO struct {
	CreateSession func() (*gocql.Session, error)

	keyspace  string
	table     string
	blockSize uint64
}

func NewStreamDAO(keyspace, table string, blockSize uint64, CreateSession func() (*gocql.Session, error)) *StreamDAO {
	dao := StreamDAO{CreateSession: CreateSession, keyspace: keyspace, table: table, blockSize: blockSize}
	if keyspace == "" {
		dao.keyspace = "test"
	}
	if table == "" {
		dao.table = "Storage"
	}
	if blockSize < minBlockSize {
		dao.blockSize = minBlockSize
	}
	return &dao
}

func (s *StreamDAO) Init(replicationFactor int64) error {
	session, err := s.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	if err = s.createKeyspaceIfMissing(session, replicationFactor); err != nil {
		return err
	}

	return s.createTableIfMissing(session)
}

func (s *StreamDAO) createKeyspaceIfMissing(session *gocql.Session, replicationFactor int64) error {
	return session.Query(fmt.Sprintf("CREATE KEYSPACE IF NOT EXISTS %v WITH REPLICATION = {'class' : 'SimpleStrategy', 'replication_factor': %v}", s.keyspace, replicationFactor)).Exec()
}

func (s *StreamDAO) createTableIfMissing(session *gocql.Session) error {
	return session.Query(fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %v.%v (
      fileName text static,
		  fileMimeType text static,
		  fileModifiedTime timestamp static,
      fileHash text,
      fileSize int static,
      fileBlocks int static,
      blockHash text,
      blockIndex int,
      blockOffset int,
      blockSize int,
      block blob,
      PRIMARY KEY (fileHash, blockOffset)
    ) WITH CLUSTERING ORDER BY (blockOffset ASC)`, s.keyspace, s.table)).Exec()
}

func (s *StreamDAO) addBlock(session *gocql.Session, fileName, fileMimeType, fileHash, blockHash string, fileSize int64, fileBlocks, blockIndex, blockOffset, blockSize uint64, fileModifiedTime time.Time, block []byte) error {
	return session.Query(fmt.Sprintf(`INSERT INTO %v.%v
    (fileName, fileMimeType, fileHash, blockHash, fileSize, fileBlocks, blockIndex, blockOffset, blockSize, fileModifiedTime, block)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.keyspace, s.table),
		fileName, fileMimeType, fileHash, blockHash, fileSize, fileBlocks, blockIndex, blockOffset, blockSize, fileModifiedTime, block).Exec()
}

func (s *StreamDAO) compute(fileName string) (string, int64, uint64, string, string, time.Time) {
	file, err := os.Open(filepath.Clean(fileName))
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	ext := strings.ToLower(path.Ext(file.Name()))
	mimeType := mime.TypeByExtension(ext)
	fileNameNoExt := filepath.Base(file.Name())

	hash := sha1.New()
	hw := bufio.NewWriter(hash)
	io.Copy(hw, bufio.NewReader(file))
	hw.Flush()

	info, _ := file.Stat()
	filesize := info.Size()
	modifiedTime := info.ModTime().UTC()
	blocks := uint64(math.Ceil(float64(filesize) / float64(s.blockSize)))

	return fileNameNoExt, filesize, blocks, hex.EncodeToString(hash.Sum(nil)), mimeType, modifiedTime
}

func (s *StreamDAO) AddFile(path string, session *gocql.Session) (string, string, error) {
	fileName, fileSize, fileBlocks, fileHash, fileMimeType, fileModifiedTime := s.compute(path)
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	if session == nil {
		session, err = s.CreateSession()
		if err != nil {
			return "", "", err
		}
		defer session.Close()
	}

	reader := bufio.NewReader(file)
	for i := uint64(0); i < fileBlocks; i++ {
		blockSize := uint64(math.Min(float64(s.blockSize), float64(fileSize-int64(i*s.blockSize))))
		block := make([]byte, blockSize)
		reader.Read(block)

		blockHash := fileHash
		if fileBlocks > 1 { //If the file fits into 1 block, no need to re-compute the hash
			bHash := sha1.New()
			bHash.Write(block)
			blockHash = hex.EncodeToString(bHash.Sum(nil))
		}

		if err = s.addBlock(session, fileName, fileMimeType, fileHash, blockHash, fileSize, fileBlocks, i, i*s.blockSize, blockSize, fileModifiedTime, block); err != nil {
			return "", "", err
		}
	}
	return fileHash, fileName, nil
}

func (s *StreamDAO) RangeRequest(fileHash string, res http.ResponseWriter, req *http.Request) error {
	r := _range.FindStringSubmatch(req.Header.Get("Range"))
	if len(r) == 0 {
		return errors.New("Request was not a range request")
	}

	session, err := s.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var (
		buf          []byte
		offset       int64
		fileSize     int64
		blockSize    int64
		fileMimeType string
	)

	if err = session.Query(fmt.Sprintf(`SELECT block, blockOffset, blockSize, fileSize, fileMimeType FROM %v.%v
		 WHERE fileHash=? AND blockOffset>=? LIMIT 1`, s.keyspace, s.table), fileHash, r[1]).Scan(&buf, &offset, &blockSize, &fileSize, &fileMimeType); err != nil {
		return err
	}

	if len(buf) == 0 {
		return errors.New("Empty result")
	}

	res.Header().Add("Accept-Ranges", "bytes")
	res.Header().Add("Content-Type", fileMimeType)
	res.Header().Add("Content-Length", strconv.FormatInt(blockSize, 10))
	res.Header().Add("Content-Range", "bytes "+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+blockSize-1, 10)+"/"+strconv.FormatInt(fileSize, 10))
	res.WriteHeader(http.StatusPartialContent)
	res.Write(buf)
	return nil
}

func (s *StreamDAO) FullRequest(fileHash string, res http.ResponseWriter, req *http.Request, headers func(int64, string, time.Time, http.ResponseWriter)) error {
	session, err := s.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var (
		buf              []byte
		fileSize         int64
		fileMimeType     string
		fileModifiedTime time.Time
	)

	if iter := session.Query(fmt.Sprintf(`SELECT block, fileSize, fileMimeType, fileModifiedTime FROM %v.%v WHERE fileHash=?`, s.keyspace, s.table), fileHash).Iter(); iter == nil {
		return errors.New("Empty Result")
	} else {
		sentHeaders := false
		for iter.Scan(&buf, &fileSize, &fileMimeType, &fileModifiedTime) {
			if len(buf) == 0 {
				return errors.New("Empty result")
			}

			if !sentHeaders {
				res.Header().Add("Content-Type", fileMimeType)
				res.Header().Add("Content-Length", strconv.FormatInt(fileSize, 10))
				res.Header().Add("Last-Modified", fileModifiedTime.Format(http.TimeFormat))
				if headers != nil {
					headers(fileSize, fileHash, fileModifiedTime, res)
				}
				res.WriteHeader(http.StatusOK)
				sentHeaders = true
			}

			res.Write(buf)
		}

		return iter.Close()
	}
}

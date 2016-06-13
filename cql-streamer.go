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
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"net/http"
	"os"
	"regexp"
	"strconv"

	"github.com/gocql/gocql"
)

const minBlockSize = 4096 // we settle for 4KB
var _range = regexp.MustCompile(`bytes=(\d+)-(\d*)`)

type StreamDAO struct {
	cluster   *gocql.ClusterConfig
	keyspace  string
	table     string
	blockSize uint64
}

func NewStreamDAO(cluster *gocql.ClusterConfig, keyspace, table string, blockSize uint64) *StreamDAO {
	dao := StreamDAO{cluster: cluster, keyspace: keyspace, table: table, blockSize: blockSize}
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
	session, err := s.cluster.CreateSession()
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

func (s *StreamDAO) addBlock(session *gocql.Session, fileName, fileHash, blockHash string, fileSize int64, fileBlocks, blockIndex, blockOffset, blockSize uint64, block []byte) error {
	return session.Query(fmt.Sprintf(`INSERT INTO %v.%v
    (fileName, fileHash, blockHash, fileSize, fileBlocks, blockIndex, blockOffset, blockSize, block)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`, s.keyspace, s.table),
		fileName, fileHash, blockHash, fileSize, fileBlocks, blockIndex, blockOffset, blockSize, block).Exec()
}

func (s *StreamDAO) compute(fileName string) (string, int64, uint64, string) {
	file, err := os.Open(fileName)
	if err != nil {
		panic(err.Error())
	}
	defer file.Close()

	// calculate the file size
	info, _ := file.Stat()
	filesize := info.Size()
	blocks := uint64(math.Ceil(float64(filesize) / float64(s.blockSize)))
	hash := sha1.New()

	for i := uint64(0); i < blocks; i++ {
		blocksize := int(math.Min(float64(s.blockSize), float64(filesize-int64(i*s.blockSize))))
		buf := make([]byte, blocksize)

		file.Read(buf)
		hash.Write(buf) // append into the hash
	}

	return file.Name(), filesize, blocks, hex.EncodeToString(hash.Sum(nil))
}

func (s *StreamDAO) AddFile(fullFileName string) error {
	fileName, fileSize, fileBlocks, fileHash := s.compute(fullFileName)
	file, err := os.Open(fullFileName)
	if err != nil {
		return err
	}
	defer file.Close()

	session, err := s.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	for i := uint64(0); i < fileBlocks; i++ {
		blockSize := uint64(math.Min(float64(s.blockSize), float64(fileSize-int64(i*s.blockSize))))
		block := make([]byte, blockSize)
		file.Read(block)

		bHash := sha1.New()
		bHash.Write(block)
		blockHash := hex.EncodeToString(bHash.Sum(nil))
		if err = s.addBlock(session, fileName, fileHash, blockHash, fileSize, fileBlocks, i, i*s.blockSize, blockSize, block); err != nil {
			return err
		}
	}
	return nil
}

func (s *StreamDAO) RangeRequest(fileHash string, res http.ResponseWriter, req *http.Request) error {
	r := _range.FindStringSubmatch(req.Header.Get("Range"))

	session, err := s.cluster.CreateSession()
	if err != nil {
		return err
	}
	defer session.Close()

	var (
		buf       []byte
		offset    int64
		fileSize  int64
		blockSize int64
	)

	if err = session.Query("SELECT block, blockOffset, blockSize, fileSize FROM test.Storage WHERE fileHash=? AND blockOffset>=? LIMIT 1", fileHash, r[1]).Scan(&buf, &offset, &blockSize, &fileSize); err != nil {
		return err
	}

	if len(buf) == 0 {
		return errors.New("Empty result")
	}

	res.Header().Add("Accept-Ranges", "bytes")
	res.Header().Add("Content-Type", "audio/mpeg")
	res.Header().Add("Content-Length", strconv.FormatInt(blockSize, 10))
	res.Header().Add("Content-Range", "bytes "+strconv.FormatInt(offset, 10)+"-"+strconv.FormatInt(offset+blockSize-1, 10)+"/"+strconv.FormatInt(fileSize, 10))
	res.WriteHeader(http.StatusPartialContent)
	res.Write(buf)
	return nil
}

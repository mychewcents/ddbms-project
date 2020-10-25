package config

type CassandraConfig struct {
	Hosts            []string `xml:"Hosts>Host"`
	Keyspace         string   `xml:"Keyspace"`
	WriteConsistency string   `xml:"WriteConsistency"`
	ReadConsistency  string   `xml:"ReadConsistency"`
}

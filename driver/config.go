package driver

type Config struct {
	// includes host:port, makes handling redirects easier
	Host     string
	UserName string
	Password string
	Database string
	Schema   string
	Role     string
}

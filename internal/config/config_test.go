package config

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

const key = "JOBQUEUE_API_KEYS"

func TestDefaultsWithKeys(t *testing.T) {
	c, err := Load(nil, env(map[string]string{key: "k0123456789abcdef:t:admin"}), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if c.Addr != ":8080" || c.AdminAddr != "127.0.0.1:9090" || c.Sync != "always" || !c.LogJSON {
		t.Errorf("defaults: %+v", c)
	}
	if !strings.HasPrefix(c.AdminAddr, "127.0.0.1") {
		t.Error("admin listener must default to loopback")
	}
}

func TestRefusesToStartWithoutAuth(t *testing.T) {
	if _, err := Load(nil, env(nil), io.Discard); err == nil || !strings.Contains(err.Error(), "authentication") {
		t.Fatalf("err = %v", err)
	}
	if _, err := Load([]string{"-insecure-no-auth"}, env(nil), io.Discard); err != nil {
		t.Fatalf("explicit insecure mode rejected: %v", err)
	}
	if _, err := Load([]string{"-insecure-no-auth"}, env(map[string]string{key: "k0123456789abcdef:t:admin"}), io.Discard); err == nil {
		t.Fatal("insecure flag combined with keys must be rejected")
	}
}

func TestFlagBeatsEnvBeatsDefault(t *testing.T) {
	e := env(map[string]string{key: "x", "JOBQUEUE_ADDR": ":9000"})
	c, _ := Load(nil, e, io.Discard)
	if c.Addr != ":9000" {
		t.Errorf("env ignored: %s", c.Addr)
	}
	c, _ = Load([]string{"-addr", ":7000"}, e, io.Discard)
	if c.Addr != ":7000" {
		t.Errorf("flag ignored: %s", c.Addr)
	}
}

func TestKeysFromFileAndSecretsNeverFlags(t *testing.T) {
	f := filepath.Join(t.TempDir(), "keys")
	os.WriteFile(f, []byte("filekey0123456789:t:admin\n"), 0o600)
	c, err := Load(nil, env(map[string]string{"JOBQUEUE_API_KEYS_FILE": f}), io.Discard)
	if err != nil || c.APIKeys != "filekey0123456789:t:admin" {
		t.Fatalf("%q %v", c.APIKeys, err)
	}
	if _, err := Load([]string{"-api-keys", "x"}, env(nil), io.Discard); err == nil {
		t.Fatal("API keys must not be accepted as a flag")
	}
}

func TestValidation(t *testing.T) {
	base := map[string]string{key: "x"}
	for _, args := range [][]string{
		{"-sync", "sometimes"}, {"-tls-cert", "a.pem"}, {"-default-partitions", "0"},
		{"-tenant-rate", "-1"}, {"-max-body-bytes", "10"}, {"-addr", ""},
	} {
		if _, err := Load(args, env(base), io.Discard); err == nil {
			t.Errorf("%v accepted", args)
		}
	}
}

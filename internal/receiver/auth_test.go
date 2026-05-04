package receiver

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"testing"
)

func sign(t *testing.T, secret string, body []byte) string {
	t.Helper()
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

func TestVerifyHMACSHA256(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	secret := "shh"
	good := sign(t, secret, body)

	if err := verifyHMACSHA256(good, secret, body); err != nil {
		t.Errorf("good signature rejected: %v", err)
	}
	if err := verifyHMACSHA256("sha256="+good, secret, body); err != nil {
		t.Errorf("prefixed signature rejected: %v", err)
	}
	if err := verifyHMACSHA256("nope", secret, body); err == nil {
		t.Error("bad signature accepted")
	}
	if err := verifyHMACSHA256("", secret, body); err == nil {
		t.Error("empty signature accepted")
	}
	if err := verifyHMACSHA256("anything", "", body); err != nil {
		t.Errorf("empty secret should skip verification: %v", err)
	}
}

func TestVerifyBearer(t *testing.T) {
	if err := verifyBearer("Bearer secret", "secret"); err != nil {
		t.Errorf("good bearer rejected: %v", err)
	}
	if err := verifyBearer("Bearer wrong", "secret"); err == nil {
		t.Error("wrong bearer accepted")
	}
	if err := verifyBearer("secret", "secret"); err == nil {
		t.Error("missing prefix accepted")
	}
	if err := verifyBearer("", ""); err != nil {
		t.Errorf("empty expected should skip: %v", err)
	}
}

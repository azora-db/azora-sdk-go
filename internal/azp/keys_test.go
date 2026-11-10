package azp

import (
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

var t0 = time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC)

func fixedKey() KeyPair {
	var k KeyPair
	for i := range k.Private {
		k.Private[i] = byte(i + 1)
	}
	pub, err := publicKey(k.Private)
	if err != nil {
		panic(err)
	}
	k.Public, k.ID, k.CreatedAt = pub, KeyID(pub), t0
	return k
}

// Golden values, cross-checked with Python's hashlib.blake2s and the
// cryptography package's X25519. SDKs in other languages must match them.
func TestKeyGoldenValues(t *testing.T) {
	k := fixedKey()
	require.Equal(t, "B6N8vBQgk8i3VdwbEOhstCY3StFqqFPtC9_AsrhtHHw", EncodePublicKey(k.Public))
	require.Equal(t, "fd1076c6", k.ID)
	require.Equal(t, "azp1-7uih-nrqh-26rp-caj3-az5k-zdaw", Fingerprint(k.Public))

	pub, err := DecodePublicKey(" B6N8vBQgk8i3VdwbEOhstCY3StFqqFPtC9_AsrhtHHw\n")
	require.NoError(t, err)
	require.Equal(t, k.Public, pub)
}

func TestGenerateKey(t *testing.T) {
	a, err := GenerateKey(t0)
	require.NoError(t, err)
	b, err := GenerateKey(t0)
	require.NoError(t, err)
	require.NotEqual(t, a.Private, b.Private)
	require.NotEqual(t, a.Public, b.Public)
	pub, err := publicKey(a.Private)
	require.NoError(t, err)
	require.Equal(t, pub, a.Public)
	require.Regexp(t, `^[0-9a-f]{8}$`, a.ID)
	require.Equal(t, KeyID(a.Public), a.ID)
	require.Regexp(t, `^azp1(-[a-z2-7]{4}){6}$`, Fingerprint(a.Public))
	require.True(t, a.CreatedAt.Equal(t0))
	require.Nil(t, a.RetiredAt)
}

func TestDecodePublicKeyErrors(t *testing.T) {
	for _, bad := range []string{"", "!!!!", "AAAA", base64.RawURLEncoding.EncodeToString(make([]byte, 33)),
		base64.StdEncoding.EncodeToString(make([]byte, 32))} {
		_, err := DecodePublicKey(bad)
		require.Error(t, err, bad)
	}
}

func TestKeyPairPrintingHidesPrivateKey(t *testing.T) {
	k := fixedKey()
	secrets := []string{
		hex.EncodeToString(k.Private[:]),
		base64.RawURLEncoding.EncodeToString(k.Private[:]),
		fmt.Sprint(k.Private[:]),
		strings.Trim(fmt.Sprint(k.Private), "[]"),
	}
	for _, f := range []string{"%v", "%+v", "%#v", "%s"} {
		s := fmt.Sprintf(f, k)
		for _, secret := range secrets {
			require.NotContains(t, s, secret, f)
		}
		require.Contains(t, s, k.ID)
	}
}

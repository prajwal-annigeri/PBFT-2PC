package cryptox

import (
	"crypto"
	"crypto/md5"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha256"
	"crypto/x509"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"log"
	"os"

	"google.golang.org/protobuf/proto"
)

func GenerateRSAKeys() (*rsa.PrivateKey, *rsa.PublicKey) {
	// Generate a new RSA private key
	privateKey, err := rsa.GenerateKey(rand.Reader, 2048) // 2048-bit key
	if err != nil {
		fmt.Println("Error generating RSA keys:", err)
		return nil, nil
	}

	// Extract the public key from the private key
	publicKey := &privateKey.PublicKey

	return privateKey, publicKey
}

func SaveRSAPublicKeyToFile(publicKey *rsa.PublicKey, filePath string) error {
	// Marshal the public key to PKIX, ASN.1 DER format
	
	publicKeyBytes, err := x509.MarshalPKIXPublicKey(publicKey)
	if err != nil {
		return fmt.Errorf("failed to marshal public key: %v", err)
	}

	// Create a PEM block with the public key bytes
	publicKeyPEM := pem.EncodeToMemory(&pem.Block{
		Type:  "RSA PUBLIC KEY",
		Bytes: publicKeyBytes,
	})

	// Write the PEM-encoded public key to a file
	err = os.WriteFile(filePath, publicKeyPEM, 0644)
	if err != nil {
		return fmt.Errorf("failed to write public key to file: %v", err)
	}
	log.Println("Wrote public key to file")

	return nil
}

func ReadRSAPublicKeyFromFile(filePath string) (*rsa.PublicKey, error) {
    // log.Printf("Reading pub key from %s\n", filePath)  
    publicKeyPEM, err := os.ReadFile(filePath)
    if err != nil {
        return nil, err
    }


    // Decode the PEM block
    block, _ := pem.Decode(publicKeyPEM)
    if block == nil || block.Type != "RSA PUBLIC KEY" {
        return nil, fmt.Errorf("failed to decode PEM block containing public key")
    }

    // Parse the DER-encoded public key
    pub, err := x509.ParsePKIXPublicKey(block.Bytes)
    if err != nil {
        return nil, fmt.Errorf("failed to parse public key: %v", err)
    }

    // Type assert to rsa.PublicKey
    publicKey, ok := pub.(*rsa.PublicKey)
    if !ok {
        return nil, fmt.Errorf("not an RSA public key")
    }
    return publicKey, nil
}

func SignMessage(privateKey *rsa.PrivateKey, message proto.Message) ([]byte, error) {
	// Serialize the message to bytes, excluding the signature field
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal message: %v", err)
	}

	// Hash the serialized message
	hashed := sha256.Sum256(messageBytes)

	// Sign the hash
	signature, err := rsa.SignPKCS1v15(rand.Reader, privateKey, crypto.SHA256, hashed[:])
	if err != nil {
		return nil, fmt.Errorf("failed to sign message: %v", err)
	}
	return signature, nil
}

func VerifySignature(publicKey *rsa.PublicKey, message proto.Message, signature []byte) error {
	// Serialize the message (excluding the signature field)
	messageBytes, err := proto.Marshal(message)
	if err != nil {
		return fmt.Errorf("failed to marshal message: %v", err)
	}

	// Hash the serialized message
	hashed := sha256.Sum256(messageBytes)

	// Verify the signature
	err = rsa.VerifyPKCS1v15(publicKey, crypto.SHA256, hashed[:], signature)
	if err != nil {
		return fmt.Errorf("verification failed: %v", err)
	}
	return nil
}

func MD5Hash(obj interface{}) (string, error) {
    // Serialize the object to JSON
    jsonData, err := json.Marshal(obj)
    if err != nil {
        return "", fmt.Errorf("failed to serialize object: %v", err)
    }

    // Create a new MD5 hash and write the JSON data to it
    hash := md5.New()
    hash.Write(jsonData)

    // Return the hash as a byte slice
	hashBytes := hash.Sum(nil)
	hashString := hex.EncodeToString(hashBytes)
	return hashString, nil
}

func SerializeRSAKey(privateKey *rsa.PrivateKey) ([]byte, error) {
    keyBytes := x509.MarshalPKCS1PrivateKey(privateKey)
    pemKey := pem.EncodeToMemory(
        &pem.Block{
            Type:  "RSA PRIVATE KEY",
            Bytes: keyBytes,
        },
    )
    if pemKey == nil {
        return nil, errors.New("failed to encode private key to PEM")
    }
    return pemKey, nil
}

func DeserializeRSAKey(pemKey []byte) (*rsa.PrivateKey, error) {
    block, _ := pem.Decode(pemKey)
    if block == nil || block.Type != "RSA PRIVATE KEY" {
        return nil, errors.New("failed to decode PEM block containing private key")
    }

    privateKey, err := x509.ParsePKCS1PrivateKey(block.Bytes)
    if err != nil {
        return nil, err
    }
    return privateKey, nil
}
package store

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/google/uuid"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/rhiza"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

func releaseProviders() []string {
	providers := []string{"filesystem"}
	if os.Getenv("COMPOS_RELEASE_S3_ENDPOINT") != "" {
		providers = append(providers, "s3")
	}
	if os.Getenv("COMPOS_RELEASE_GCS_ENDPOINT") != "" {
		providers = append(providers, "gcs")
	}
	if os.Getenv("COMPOS_RELEASE_AZURE_ENDPOINT") != "" {
		providers = append(providers, "azure")
	}
	return providers
}

func configureReleaseProvider(t *testing.T, ctx context.Context, provider string, config *rhiza.Config) {
	t.Helper()
	config.ObjStoreProvider = provider
	if provider == "filesystem" {
		config.ObjStoreDir = t.TempDir()
		return
	}
	endpoint := os.Getenv("COMPOS_RELEASE_" + map[string]string{"s3": "S3", "gcs": "GCS", "azure": "AZURE"}[provider] + "_ENDPOINT")
	host, _, err := net.SplitHostPort(endpoint)
	if err != nil || host != "127.0.0.1" {
		t.Fatal("release provider endpoint must be an isolated 127.0.0.1 endpoint")
	}
	bucket := "compos-release-" + uuid.NewString()
	config.ObjStoreBucket = bucket
	if provider == "gcs" {
		t.Setenv("STORAGE_EMULATOR_HOST", "http://"+endpoint)
		tokenServer := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`{"access_token":"emulator-only","token_type":"Bearer","expires_in":3600}`))
		}))
		t.Cleanup(tokenServer.Close)
		credentialDir := t.TempDir()
		tokenFile := filepath.Join(credentialDir, "subject-token")
		if err := os.WriteFile(tokenFile, []byte("emulator-only"), 0600); err != nil {
			t.Fatal(err)
		}
		credentialFile := filepath.Join(credentialDir, "credentials.json")
		credential := fmt.Sprintf(`{"type":"external_account","audience":"//emulator.invalid/test","subject_token_type":"urn:ietf:params:oauth:token-type:jwt","token_url":%q,"credential_source":{"file":%q}}`, tokenServer.URL, tokenFile)
		if err := os.WriteFile(credentialFile, []byte(credential), 0600); err != nil {
			t.Fatal(err)
		}
		t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", credentialFile)
		client, err := storage.NewClient(ctx, option.WithoutAuthentication())
		if err != nil {
			t.Fatal(err)
		}
		if err := client.Bucket(bucket).Create(ctx, "compos-test", nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			it := client.Bucket(bucket).Objects(ctx, nil)
			for {
				obj, err := it.Next()
				if err == iterator.Done {
					break
				}
				if err != nil {
					t.Error(err)
					break
				}
				if err := client.Bucket(bucket).Object(obj.Name).Delete(ctx); err != nil {
					t.Error(err)
				}
			}
			if err := client.Bucket(bucket).Delete(ctx); err != nil {
				t.Error(err)
			}
			if err := client.Close(); err != nil {
				t.Error(err)
			}
		})
		return
	}
	if provider == "azure" {
		conn := fmt.Sprintf("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Y29tcG9z;BlobEndpoint=http://%s/devstoreaccount1;", endpoint)
		client, err := azblob.NewClientFromConnectionString(conn, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := client.CreateContainer(ctx, bucket, nil); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			if _, err := client.DeleteContainer(ctx, bucket, nil); err != nil {
				t.Error(err)
			}
		})
		config.ObjStoreAzureConnectionString = conn
		config.ObjStoreAzureStorageAccount = "devstoreaccount1"
		return
	}
	client, err := minio.New(endpoint, &minio.Options{Creds: credentials.NewStaticV4("compos", "compos-secret", "")})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.MakeBucket(ctx, bucket, minio.MakeBucketOptions{Region: "us-east-1"}); err != nil {
		t.Fatalf("create isolated bucket: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		for object := range client.ListObjects(ctx, bucket, minio.ListObjectsOptions{Recursive: true}) {
			if object.Err != nil {
				t.Error(object.Err)
				continue
			}
			if err := client.RemoveObject(ctx, bucket, object.Key, minio.RemoveObjectOptions{}); err != nil {
				t.Error(err)
			}
		}
		if err := client.RemoveBucket(ctx, bucket); err != nil {
			t.Error(err)
		}
	})
	config.ObjStoreEndpoint, config.ObjStoreBucket = endpoint, bucket
	config.ObjStoreRegion, config.ObjStoreInsecure = "us-east-1", true
	config.ObjStoreAccessKey, config.ObjStoreSecretKey = "compos", "compos-secret"
}

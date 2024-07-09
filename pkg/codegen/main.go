package main

import (
	"fmt"
	"os"

	v12 "github.com/cnrancher/tke-operator/pkg/apis/tke.pandaria.io/v1"
	_ "github.com/rancher/wrangler-api/pkg/generated/controllers/apiextensions.k8s.io"
	controllergen "github.com/rancher/wrangler/v3/pkg/controller-gen"
	"github.com/rancher/wrangler/v3/pkg/controller-gen/args"
	"github.com/rancher/wrangler/v3/pkg/crd"
	"github.com/rancher/wrangler/v3/pkg/yaml"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func main() {
	os.Unsetenv("GOPATH")

	controllergen.Run(args.Options{
		OutputPackage: "github.com/cnrancher/tke-operator/pkg/generated",
		Boilerplate:   "pkg/codegen/boilerplate.go.txt",
		Groups: map[string]args.Group{
			"tke.pandaria.io": {
				Types: []interface{}{
					"./pkg/apis/tke.pandaria.io/v1",
				},
				GenerateTypes: true,
			},
			corev1.GroupName: {
				Types: []interface{}{
					corev1.Pod{},
					corev1.Node{},
					corev1.Secret{},
				},
			},
		},
	})

	tkeClusterConfig := newCRD(&v12.TKEClusterConfig{}, func(c crd.CRD) crd.CRD {
		c.ShortNames = []string{"tkecc"}
		return c
	})

	obj, err := tkeClusterConfig.ToCustomResourceDefinition()
	if err != nil {
		panic(err)
	}

	obj.(*unstructured.Unstructured).SetAnnotations(map[string]string{
		"helm.sh/resource-policy": "keep",
	})

	tkeCCYaml, err := yaml.Export(obj)
	if err != nil {
		panic(err)
	}

	if err := saveCRDYaml("tke-operator-crd", string(tkeCCYaml)); err != nil {
		panic(err)
	}

	fmt.Printf("obj yaml: %s", tkeCCYaml)
}

func newCRD(obj interface{}, customize func(crd.CRD) crd.CRD) crd.CRD {
	crd := crd.CRD{
		GVK: schema.GroupVersionKind{
			Group:   "tke.pandaria.io",
			Version: "v1",
		},
		Status:       true,
		SchemaObject: obj,
	}
	if customize != nil {
		crd = customize(crd)
	}
	return crd
}

func saveCRDYaml(name, yaml string) error {
	filename := fmt.Sprintf("./charts/%s/templates/crds.yaml", name)
	save, err := os.Create(filename)
	if err != nil {
		return err
	}

	defer save.Close()
	if err := save.Chmod(0755); err != nil {
		return err
	}

	if _, err := fmt.Fprint(save, yaml); err != nil {
		return err
	}

	return nil
}

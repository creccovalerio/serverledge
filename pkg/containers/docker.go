package containers

import (
	"context"
	"io"
	"log"
	"os"

	"github.com/docker/docker/client"
	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/pkg/stdcopy"
)

type DockerFactory struct {
	cli *client.Client
	ctx context.Context
}

func InitDockerContainerFactory() {
	ctx := context.Background()
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		panic(err)
	}

	cf = &DockerFactory{cli, ctx}
}

func (cf *DockerFactory) Create (image string, opts *ContainerOptions) (ContainerID, error) {
	reader, err := cf.cli.ImagePull(cf.ctx, image, types.ImagePullOptions{})
	if err != nil {
		return "", err
	}
	io.Copy(os.Stdout, reader) // TODO

	resp, err := cf.cli.ContainerCreate(cf.ctx, &container.Config{
		Image: image,
		Cmd:   []string{"echo", "hello world"},
		Tty:   false,
	}, nil, nil, nil, "")

	return resp.ID, err
}

func (cf *DockerFactory) Start (contID ContainerID, cmd []string) error {
	if err := cf.cli.ContainerStart(cf.ctx, contID, types.ContainerStartOptions{}); err != nil {
		return err
	}

	statusCh, errCh := cf.cli.ContainerWait(cf.ctx, contID, container.WaitConditionNotRunning)
	select {
	case err := <-errCh:
		if err != nil {
			return err
		}
	case <-statusCh:
	}

	out, err := cf.cli.ContainerLogs(cf.ctx, contID, types.ContainerLogsOptions{ShowStdout: true})
	if err != nil {
		return err
	}
	log.Printf("Type of out: %T", out)

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)
	return nil
}


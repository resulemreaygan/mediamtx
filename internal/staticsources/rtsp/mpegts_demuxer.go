package rtsp

import (
	"errors"
	"fmt"
	"io"

	"github.com/bluenviron/gortsplib/v4"
	"github.com/bluenviron/gortsplib/v4/pkg/description"
	"github.com/bluenviron/gortsplib/v4/pkg/format"
	"github.com/pion/rtp"

	"github.com/bluenviron/mediamtx/internal/counterdumper"
	"github.com/bluenviron/mediamtx/internal/defs"
	"github.com/bluenviron/mediamtx/internal/logger"
	"github.com/bluenviron/mediamtx/internal/protocols/mpegts"
	"github.com/bluenviron/mediamtx/internal/protocols/rtpmpegts"
	"github.com/bluenviron/mediamtx/internal/stream"
)

func findSingleMPEGTSFormat(desc *description.Session) (*description.Media, *format.MPEGTS) {
	if len(desc.Medias) != 1 || len(desc.Medias[0].Formats) != 1 {
		return nil, nil
	}

	forma, ok := desc.Medias[0].Formats[0].(*format.MPEGTS)
	if !ok {
		return nil, nil
	}

	return desc.Medias[0], forma
}

type mpegtsDemuxer struct {
	log          logger.Writer
	parent       parent
	client       *gortsplib.Client
	mpegtsMedia  *description.Media
	mpegtsFormat *format.MPEGTS
	decodeErrors *counterdumper.CounterDumper

	pipeWriter *io.PipeWriter
	errChan    chan error
}

func (d *mpegtsDemuxer) initialize() error {
	decoder := &rtpmpegts.Decoder{}
	err := decoder.Init()
	if err != nil {
		return fmt.Errorf("failed to create MPEG-TS decoder: %w", err)
	}

	pr, pw := io.Pipe()
	d.pipeWriter = pw
	d.errChan = make(chan error, 1)

	d.client.OnPacketRTP(d.mpegtsMedia, d.mpegtsFormat, func(pkt *rtp.Packet) {
		tsData, decErr := decoder.Decode(pkt)
		if decErr != nil {
			d.decodeErrors.Increase()
			return
		}

		for _, data := range tsData {
			_, err = pw.Write(data)
			if err != nil {
				return
			}
		}
	})

	go d.run(pr)

	return nil
}

func (d *mpegtsDemuxer) close() {
	if d.pipeWriter != nil {
		d.pipeWriter.CloseWithError(io.EOF)
	}
}

func (d *mpegtsDemuxer) wait() error {
	return <-d.errChan
}

func (d *mpegtsDemuxer) run(pr *io.PipeReader) {
	err := d.doRun(pr)
	if err != nil && !errors.Is(err, io.EOF) {
		d.log.Log(logger.Error, "MPEG-TS demuxer error: %v", err)
	}
	d.errChan <- err
}

func (d *mpegtsDemuxer) doRun(pr *io.PipeReader) error {
	mr := &mpegts.EnhancedReader{R: pr}
	err := mr.Initialize()
	if err != nil {
		return fmt.Errorf("failed to initialize MPEG-TS reader: %w", err)
	}

	mr.OnDecodeError(func(_ error) {
		d.decodeErrors.Increase()
	})

	var strm *stream.Stream

	medias, err := mpegts.ToStream(mr, &strm, d.log)
	if err != nil {
		return fmt.Errorf("failed to map MPEG-TS to stream: %w", err)
	}

	res := d.parent.SetReady(defs.PathSourceStaticSetReadyReq{
		Desc:               &description.Session{Medias: medias},
		GenerateRTPPackets: true,
	})
	if res.Err != nil {
		return res.Err
	}

	defer d.parent.SetNotReady(defs.PathSourceStaticSetNotReadyReq{})

	strm = res.Stream

	for {
		err = mr.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
	}
}

package org.onedatashare.transferservice.odstransferservice.service.drain;

import lombok.RequiredArgsConstructor;
import org.onedatashare.transferservice.odstransferservice.service.abstracClass.Drain;
import org.onedatashare.transferservice.odstransferservice.model.Slice;

import java.io.ByteArrayOutputStream;
@RequiredArgsConstructor
public class GDriveDrain extends Drain {

    @Override
    public void drain(Slice slice) throws Exception {

    }

    @Override
    public void finish() throws Exception {

    }

    @Override
    public Slice getSlice() {
        return null;
    }
}

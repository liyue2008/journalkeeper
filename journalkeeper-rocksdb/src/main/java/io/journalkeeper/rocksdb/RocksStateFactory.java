package io.journalkeeper.rocksdb;

import io.journalkeeper.core.easy.JkState;
import io.journalkeeper.core.easy.JkStateFactory;

public class RocksStateFactory extends JkStateFactory {

    @Override
    protected void onStateCreated(JkState state) {
        super.onStateCreated(state);
        RocksState rocksState = new RocksState();

        state.onRecover(rocksState::recover);
        state.onClose(rocksState::close);

        state.registerQueryCommandHandler(RocksOperation.GET, rocksState::get);
        state.registerExecuteCommandHandler(RocksOperation.DEL, rocksState::del);
        state.registerExecuteCommandHandler(RocksOperation.PUT, rocksState::put);
    }
}

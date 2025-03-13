package org.apache.hadoop.ozone.containerlog.parser;

import org.apache.hadoop.hdds.utils.db.Table;
import org.apache.hadoop.hdds.utils.db.TableIterator;
import java.io.IOException;
import java.util.NoSuchElementException;

public class ContainerLogTableIterator <KEY, VALUE> implements TableIterator<KEY, Table.KeyValue<KEY, VALUE>> {

  private final TableIterator<KEY, ? extends Table.KeyValue<KEY, VALUE>> iter;
  private Table.KeyValue<KEY, VALUE> nextTx;

  public ContainerLogTableIterator(ContainerLogTable<KEY, VALUE> table) throws IOException {
    this.iter = table.getTable().iterator();
    findNext();
  }

  private void findNext() {
    if (iter.hasNext()) {
      nextTx = iter.next();
    } else{
      nextTx = null;
    }
  }

  @Override
  public boolean hasNext() {
    return nextTx != null;
  }

  @Override
  public Table.KeyValue<KEY, VALUE> next() {
    if (nextTx == null) {
      throw new NoSuchElementException();
    }
    Table.KeyValue<KEY, VALUE> current = nextTx;
    findNext();
    return current;
  }

  @Override
  public void close() throws IOException {
    iter.close();
  }

  @Override
  public void seekToFirst() {

  }

  @Override
  public void seekToLast() {

  }

  @Override
  public Table.KeyValue<KEY, VALUE> seek(KEY key) throws IOException {
    return null;
  }

  @Override
  public void removeFromDB() throws IOException {

  }
}

package org.apache.hadoop.ozone.recon.persistence;

import static org.apache.hadoop.ozone.recon.ReconControllerModule.ReconDaoBindingModule.RECON_DAO_LIST;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.SQLException;

import org.hadoop.ozone.recon.schema.tables.daos.ReconTaskStatusDao;
import org.hadoop.ozone.recon.schema.tables.pojos.ReconTaskStatus;
import org.junit.Test;

/**
 * Class to test basic SQL schema setup.
 */
public class TestSqlSchemaSetup extends AbstractReconSqlDBTest {

  /**
   * Make sure schema was created correctly.
   * @throws SQLException
   */
  @Test
  public void testSchemaSetup() throws SQLException {
    assertNotNull(getInjector());
    assertNotNull(getConfiguration());
    assertNotNull(getDslContext());
    assertNotNull(getConnection());
    RECON_DAO_LIST.forEach(dao -> {
      assertNotNull(getDao(dao));
    });
    ReconTaskStatusDao dao = getDao(ReconTaskStatusDao.class);
    dao.insert(new ReconTaskStatus("TestTask", 1L, 2L));
    assertEquals(1, dao.findAll().size());
  }
}

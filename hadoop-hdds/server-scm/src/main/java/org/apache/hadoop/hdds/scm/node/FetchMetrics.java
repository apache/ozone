package org.apache.hadoop.hdds.scm.node;

import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.lang.reflect.Array;
import java.util.Iterator;
import java.util.Set;
import javax.management.AttributeNotFoundException;
import javax.management.InstanceNotFoundException;
import javax.management.IntrospectionException;
import javax.management.MBeanAttributeInfo;
import javax.management.MBeanException;
import javax.management.MBeanInfo;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.ReflectionException;
import javax.management.RuntimeErrorException;
import javax.management.RuntimeMBeanException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.CompositeType;
import javax.management.openmbean.TabularData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Class used to fetch metrics from MBeanServer.
 */
public class FetchMetrics {
  private static final Logger LOG = LoggerFactory.getLogger(FetchMetrics.class);
  private transient MBeanServer mBeanServer;
  private transient JsonFactory jsonFactory;

  public FetchMetrics() {
    this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
    this.jsonFactory = new JsonFactory();
  }

  public String getMetrics(String qry) {
    try {
      JsonGenerator jg = null;
      OutputStream outputStream = new ByteArrayOutputStream();

      try {
        jg = this.jsonFactory.createGenerator(outputStream, JsonEncoding.UTF8);
        jg.disable(JsonGenerator.Feature.AUTO_CLOSE_TARGET);
        jg.useDefaultPrettyPrinter();
        jg.writeStartObject();
        if (qry == null) {
          qry = "*:*";
        }
        this.listBeans(jg, new ObjectName(qry)/*, (String) null*/);
      } finally {
        if (jg != null) {
          jg.close();
        }
      }
      return outputStream.toString();
    } catch (IOException var12) {
      LOG.error("Caught an exception while processing JMX request", var12);
    } catch (MalformedObjectNameException var13) {
      LOG.error("Caught an exception while processing JMX request", var13);
    }
    return null;
  }

  private void listBeans(JsonGenerator jg, ObjectName qry/*, String attribute*/)
      throws IOException {
    LOG.info("Listing beans for " + qry);
    Set<ObjectName> names = null;
    names = this.mBeanServer.queryNames(qry, (QueryExp) null);
    jg.writeArrayFieldStart("beans");
    Iterator<ObjectName> it = names.iterator();

    while (it.hasNext()) {
      ObjectName oname = (ObjectName) it.next();
      String code = "";
      //Object attributeinfo = null;

      MBeanInfo minfo;
      try {
        minfo = this.mBeanServer.getMBeanInfo(oname);
        code = minfo.getClassName();
        String prs = "";

        try {
          if ("org.apache.commons.modeler.BaseModelMBean".equals(code)) {
            prs = "modelerType";
            code = (String) this.mBeanServer.getAttribute(oname, prs);
          }

          /*if (attribute != null) {
            attributeinfo = this.mBeanServer.getAttribute(oname, attribute);
          }*/
        } catch (AttributeNotFoundException var13) {
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", var13);
        } catch (MBeanException var14) {
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", var14);
        } catch (RuntimeException var15) {
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", var15);
        } catch (ReflectionException var16) {
          LOG.error("getting attribute " + prs + " of " + oname + " threw an exception", var16);
        }
      } catch (InstanceNotFoundException var17) {
        continue;
      } catch (IntrospectionException var18) {
        LOG.error("Problem while trying to process JMX query: " + qry + " with MBean " + oname, var18);
        continue;
      } catch (ReflectionException var19) {
        LOG.error("Problem while trying to process JMX query: " + qry + " with MBean " + oname, var19);
        continue;
      }

      jg.writeStartObject();
      jg.writeStringField("name", oname.toString());
      jg.writeStringField("modelerType", code);
      /*if (attribute != null && attributeinfo == null) {
        jg.writeStringField("result", "ERROR");
        jg.writeStringField("message", "No attribute with name " + attribute + " was found.");
        jg.writeEndObject();
        jg.writeEndArray();
        jg.close();
        return;
      }*/

      MBeanAttributeInfo[] attrs = minfo.getAttributes();
      for (int i = 0; i < attrs.length; ++i) {
        this.writeAttribute(jg, oname, attrs[i]);
      }

      /*if (attribute != null) {
        this.writeAttribute(jg, attribute, attributeinfo);
      } else {
        MBeanAttributeInfo[] attrs = minfo.getAttributes();

        for (int i = 0; i < attrs.length; ++i) {
          this.writeAttribute(jg, oname, attrs[i]);
        }
      }*/

      jg.writeEndObject();
    }

    jg.writeEndArray();
  }

  private void writeAttribute(JsonGenerator jg, ObjectName oname, MBeanAttributeInfo attr) throws IOException {
    if (attr.isReadable()) {
      String attName = attr.getName();
      if (!"modelerType".equals(attName)) {
        if (attName.indexOf("=") < 0 && attName.indexOf(":") < 0 && attName.indexOf(" ") < 0) {
          Object value = null;

          try {
            value = this.mBeanServer.getAttribute(oname, attName);
          } catch (RuntimeMBeanException var7) {
            if (var7.getCause() instanceof UnsupportedOperationException) {
              LOG.info("getting attribute " + attName + " of " + oname + " threw an exception", var7);
            } else {
              LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", var7);
            }

            return;
          } catch (RuntimeErrorException var8) {
            LOG.error("getting attribute {} of {} threw an exception", new Object[]{attName, oname, var8});
            return;
          } catch (AttributeNotFoundException var9) {
            return;
          } catch (MBeanException var10) {
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", var10);
            return;
          } catch (RuntimeException var11) {
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", var11);
            return;
          } catch (ReflectionException var12) {
            LOG.error("getting attribute " + attName + " of " + oname + " threw an exception", var12);
            return;
          } catch (InstanceNotFoundException var13) {
            return;
          }

          this.writeAttribute(jg, attName, value);
        }
      }
    }
  }

  private void writeAttribute(JsonGenerator jg, String attName, Object value) throws IOException {
    jg.writeFieldName(attName);
    this.writeObject(jg, value);
  }

  private void writeObject(JsonGenerator jg, Object value) throws IOException {
    if (value == null) {
      jg.writeNull();
    } else {
      Class<?> c = value.getClass();
      Object entry;
      if (c.isArray()) {
        jg.writeStartArray();
        int len = Array.getLength(value);

        for (int j = 0; j < len; ++j) {
          entry = Array.get(value, j);
          this.writeObject(jg, entry);
        }

        jg.writeEndArray();
      } else if (value instanceof Number) {
        Number n = (Number) value;
        jg.writeNumber(n.toString());
      } else if (value instanceof Boolean) {
        Boolean b = (Boolean) value;
        jg.writeBoolean(b);
      } else if (value instanceof CompositeData) {
        CompositeData cds = (CompositeData) value;
        CompositeType comp = cds.getCompositeType();
        Set<String> keys = comp.keySet();
        jg.writeStartObject();
        Iterator var7 = keys.iterator();

        while (var7.hasNext()) {
          String key = (String) var7.next();
          this.writeAttribute(jg, key, cds.get(key));
        }

        jg.writeEndObject();
      } else if (value instanceof TabularData) {
        TabularData tds = (TabularData) value;
        jg.writeStartArray();
        Iterator var14 = tds.values().iterator();

        while (var14.hasNext()) {
          entry = var14.next();
          this.writeObject(jg, entry);
        }

        jg.writeEndArray();
      } else {
        jg.writeString(value.toString());
      }
    }

  }
}

package org.apache.cassandra.cql3;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryHandlerInstance
{
    private static final Logger logger = LoggerFactory.getLogger(QueryHandlerInstance.class);

    public static final QueryHandler instance;

    static
    {
        QueryHandler handler = DatabaseDescriptor.instance.getQueryProcessor();
        String customHandlerClass = System.getProperty("cassandra.custom_query_handler_class");
        if (customHandlerClass != null)
        {
            try
            {
                handler = (QueryHandler) FBUtilities.construct(customHandlerClass, "QueryHandler");
                logger.info("Using {} as query handler for native protocol queries (as requested with -Dcassandra.custom_query_handler_class)", customHandlerClass);
            }
            catch (Exception e)
            {
                logger.info("Cannot use class {} as query handler ({}), ignoring by defaulting on normal query handling", customHandlerClass, e.getMessage());
            }
        }
        instance = handler;
    }
}

package org.customdrill.drill_simple_randomco;

import com.google.common.base.Strings;

import io.netty.buffer.DrillBuf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableFloat8Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.reader.Var16CharReader;
import org.apache.drill.exec.vector.complex.reader.VarCharReader;

import javax.inject.Inject;


@FunctionTemplate(
        name = "randomco",
        scope = FunctionTemplate.FunctionScope.SIMPLE,
        nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
)
public class RandomCoordinates implements DrillSimpleFunc {

	@Param
	VarCharHolder lat_1;
	
	@Param
	VarCharHolder long_1;
    
	@Param
	VarCharHolder lat_2;
    
	@Param
	VarCharHolder long_2;
    
	@Param
	VarCharHolder lat_3;
    
	@Param
	VarCharHolder long_3;
    
	@Param
	VarCharHolder lat_4;
    
	@Param
	VarCharHolder long_4;
    
	@Output
	 VarCharHolder out;

    @Inject
    DrillBuf buffer;

    public void setup() {
    }


    public void eval() {
    //Get 8 coordinates from input and derive a random coordinate
    	 // get the value and replace with
    	float rlat = (float) 0.0;
    	float rlong = (float) 0.0;
    	
        float lat1 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(lat_1));
        float long1 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(long_1));
        
        float lat2 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(lat_2));
        float long2 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(long_2));
        
        float lat3 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(lat_3));
        float long3 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(long_3));
        
        float lat4 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(lat_4));
        float long4 = Float.parseFloat(org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers.getStringFromVarCharHolder(long_4));
        
        if(lat4 > lat2)
    	{
    		rlat = (float) (Math.random() * (lat4 - lat2) + lat2);   		
    	}
    	else
    	{
    		rlat = (float) (Math.random() * (lat2 - lat4) + lat4);
    	}
    	
    	if( long3 > long1)
		{
    		rlong = (float) Math.random() * (long3 - long1) + long1;
		}
    	else
    	{
    		 rlong = (float) Math.random() * (long1  - long3) + long3; 
    	}
    	
        
        String outputvalue = rlat + ","  + rlong;
        
     // put the output value in the out buffer
        out.buffer = buffer;
        out.start = 0;
        out.end = outputvalue.getBytes().length;
        buffer.setBytes(0, outputvalue.getBytes());
        
        
    }
         
}



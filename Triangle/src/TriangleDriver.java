import java.io.IOException;

public class TriangleDriver {
	
	public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException
	{		
		String[] GBDirArr = {args[0], args[1] + "/GraphData0"};
		GraphBuilder.main(GBDirArr);
		
		String[] GLDirArr = {args[1] + "/GraphData0", args[1] + "/GraphData1"};
		GraphLinker.main(GLDirArr);
		
		String[] TCDirArr = {args[1] + "/GraphData0", args[1] + "/GraphData1", args[1] + "/FinalResult"};
		TriangleChecker.main(TCDirArr);
	}
}

import org.apache.hadoop.util.ToolRunner;
import tool.AnalysisBeanTool;
import tool.AnalysisTextTool;

public class AnalysisData {

    public static void main(String[] args) {

        try {
            ToolRunner.run(new AnalysisTextTool(),args);
//            ToolRunner.run(new AnalysisBeanTool(),args);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

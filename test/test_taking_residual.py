import unittest
import numpy as np
import pandas as pd
from unittest.mock import patch, MagicMock, PropertyMock
from research.taking_residuals import TakingResidual  # Ensure this class is in a module that can be imported.

# Define the mean and standard deviation.
# These are chosen to ensure most values lie within [0, 1).
mean = 0.5
std_dev = 0.15

# Generate 96 normally distributed data points with the defined mean and std_dev.
data = np.random.normal(loc=mean, scale=std_dev, size=96)

# Clip the values to ensure they are within [0, 1).
data = np.clip(data, 0, 1)

# Create a pandas DataFrame with the generated data.
mock_df = pd.DataFrame({
    'timestamp': pd.date_range(start='2024-01-01', periods=96, freq='H'),  # Hourly frequency for 96 data points.
    'fee': data
})

# Mocked Decomposer and Decomposition result
mock_decomposer = MagicMock()

# Create a mock for the resid property using PropertyMock
mock_resid = PropertyMock(return_value=pd.Series(data, index=mock_df.index))
type(mock_decomposer.decompose.return_value).resid = mock_resid

# Now create a mock for the dropna method
mock_dropna = MagicMock(return_value=pd.Series(data, index=mock_df.index))
mock_decomposer.decompose.return_value.resid.dropna = mock_dropna

class TestTakingResidual(unittest.TestCase):

    @patch('pymongo.MongoClient')
    def test_init(self, mock_client):
        # Test initialization and MongoDB connection
        TakingResidual()
        mock_client.assert_called_with("mongodb://localhost:27017/")

    @patch('pymongo.MongoClient')
    def test_get_time_series_data(self, mock_client):
        # Mock database call
        mock_collection = mock_client.return_value.__getitem__.return_value.__getitem__.return_value
        mock_collection.find.return_value = [{}]  # Put here the mocked return value representing MongoDB documents

        tr = TakingResidual()
        df = tr.get_time_series_data()
        
        # Check that the DataFrame has the correct columns and types
        self.assertIn('timestamp', df.columns)
        self.assertIn('fee', df.columns)
        self.assertTrue(pd.api.types.is_datetime64_ns_dtype(df['timestamp']))
        self.assertTrue(pd.api.types.is_float_dtype(df['fee']))

    @patch('matplotlib.pyplot.figure')
    def test_plot_fee(self, mock_figure):
        # Mock pyplot figure to avoid GUI render during the test
        tr = TakingResidual()
        tr.plot_fee(mock_df)  # We can use the mock_df defined above
        mock_figure.assert_called()

    @patch('research.decomposer.Decomposer', return_value=mock_decomposer)
    def test_get_residual(self, mock_decomposer_class):
        # Test residual obtaining
        tr = TakingResidual()
        resid = tr.get_residual(mock_df)
        self.assertTrue(isinstance(resid, pd.DataFrame))

    def test_get_gbm_path(self):
        # Test GBM path generation
        tr = TakingResidual()
        resid = pd.DataFrame({'resid': np.random.normal(loc=0, scale=1, size=100)})
        gbm_path = tr.get_gbm_path(resid)
        self.assertEqual(len(gbm_path), 100)

    @patch('matplotlib.pyplot.figure')
    def test_plot_gbm_path(self, mock_figure):
        # Test GBM path plotting
        tr = TakingResidual()
        tr.plot_gbm_path([-10, -9, -8, 7, 6])  # some random gbm path points
        mock_figure.assert_called()

if __name__ == '__main__':
    unittest.main()

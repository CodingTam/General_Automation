import React, { useState, useRef, useEffect } from 'react';

const MultiSelect = ({ options, value, onChange, placeholder, className = '' }) => {
  const [isOpen, setIsOpen] = useState(false);
  const containerRef = useRef(null);
  
  // Close the dropdown when clicking outside
  useEffect(() => {
    const handleClickOutside = (event) => {
      if (containerRef.current && !containerRef.current.contains(event.target)) {
        setIsOpen(false);
      }
    };
    
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);
  
  // Toggle option selection
  const toggleOption = (optionValue) => {
    const isSelected = value.includes(optionValue);
    
    if (isSelected) {
      onChange(value.filter(v => v !== optionValue));
    } else {
      onChange([...value, optionValue]);
    }
  };
  
  return (
    <div ref={containerRef} className={`relative ${className}`}>
      <div
        className="multi-select flex justify-between items-center cursor-pointer"
        onClick={() => setIsOpen(!isOpen)}
      >
        <div className="flex-1 truncate">
          {value.length > 0 ? (
            value.join(', ')
          ) : (
            <span className="text-gray-400">{placeholder}</span>
          )}
        </div>
        <svg
          className={`w-5 h-5 text-gray-400 transition-transform duration-200 ${isOpen ? 'transform rotate-180' : ''}`}
          fill="none"
          stroke="currentColor"
          viewBox="0 0 24 24"
          xmlns="http://www.w3.org/2000/svg"
        >
          <path
            strokeLinecap="round"
            strokeLinejoin="round"
            strokeWidth="2"
            d="M19 9l-7 7-7-7"
          />
        </svg>
      </div>
      
      {isOpen && (
        <div className="multi-select-menu">
          {options.length === 0 ? (
            <div className="py-2 px-4 text-sm text-gray-500">No options available</div>
          ) : (
            options.map(option => (
              <div
                key={option.value}
                className="multi-select-option"
                data-selected={value.includes(option.value)}
                onClick={() => toggleOption(option.value)}
              >
                <span className="absolute inset-y-0 left-0 flex items-center pl-3">
                  {value.includes(option.value) && (
                    <svg
                      className="w-5 h-5 text-blue-600"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                      xmlns="http://www.w3.org/2000/svg"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth="2"
                        d="M5 13l4 4L19 7"
                      />
                    </svg>
                  )}
                </span>
                {option.label}
              </div>
            ))
          )}
        </div>
      )}
    </div>
  );
};

export default MultiSelect; 